"""Tests for scrape/parse quality indicator computation."""

import json
import os
from datetime import datetime

import pytest
from il_supermarket_scarper import DumpFolderNames, FileTypesFilters

from managers.quality_indicators import (
    PARSER_QUALITY_FILENAME,
    PIPELINE_HEALTH_FILENAME,
    SCRAPER_QUALITY_FILENAME,
    compute_parser_quality,
    compute_pipeline_health,
    compute_scraper_quality,
    write_quality_indicators,
)


def _write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, default=str)


def _scraper_status(
    task_id: str,
    started_at: datetime,
    saw_files: list[str],
    downloaded_files: list[str],
    failed_downloads: list[tuple[str, str]] | None = None,
    limit: int | None = None,
    saw_links: dict[str, str] | None = None,
) -> dict:
    saw_links = saw_links or {}
    events = [
        {
            "task_id": task_id,
            "status": "saw",
            "system_timestamp": started_at.isoformat(),
            "file_name": name,
            "link": saw_links.get(name),
            "size": None,
        }
        for name in saw_files
    ]
    events.extend(
        {
            "task_id": task_id,
            "status": "downloaded",
            "system_timestamp": started_at.isoformat(),
            "file_name": name,
            "downloaded_successfully": True,
            "extracted_successfully": True,
            "error_message": None,
            "restart_and_retry": False,
        }
        for name in downloaded_files
    )
    for file_name, error in failed_downloads or []:
        events.append(
            {
                "task_id": task_id,
                "status": "downloaded",
                "system_timestamp": started_at.isoformat(),
                "file_name": file_name,
                "downloaded_successfully": False,
                "extracted_successfully": False,
                "error_message": error,
                "restart_and_retry": False,
            }
        )
    verified = [
        {
            "task_id": task_id,
            "file_name": name,
            "system_timestamp": started_at.isoformat(),
        }
        for name in downloaded_files
    ]
    return {
        "global_status": [
            {
                "task_id": task_id,
                "status": "started",
                "system_timestamp": started_at.isoformat(),
                "limit": limit,
            }
        ],
        "events": events,
        "verified_downloads": verified,
    }


def _parser_status(
    task_id: str,
    started_at: datetime,
    processed: list[tuple[str, int]],
    skipped: list[str] | None = None,
    failed: list[str] | None = None,
) -> dict:
    events = [
        {
            "status": "processed",
            "system_timestamp": started_at.isoformat(),
            "file_name": file_name,
            "store_folder": "bareket",
            "file_type": "PRICE_FILE",
            "row_count": row_count,
            "task_id": task_id,
        }
        for file_name, row_count in processed
    ]
    for file_name in skipped or []:
        events.append(
            {
                "status": "skipped",
                "system_timestamp": started_at.isoformat(),
                "file_name": file_name,
                "store_folder": "bareket",
                "file_type": "PRICE_FILE",
                "task_id": task_id,
            }
        )
    for file_name in failed or []:
        events.append(
            {
                "status": "failed",
                "system_timestamp": started_at.isoformat(),
                "file_name": file_name,
                "store_folder": "bareket",
                "file_type": "PRICE_FILE",
                "error": "parse error",
                "trace": "",
                "row_count": 0,
                "task_id": task_id,
            }
        )
    return {
        "global_status": [
            {
                "status": "started",
                "system_timestamp": started_at.isoformat(),
                "limit": None,
                "scraper": "BAREKET",
                "files_types": "PRICE_FILE",
                "task_id": task_id,
            },
            {
                "status": "completed",
                "system_timestamp": started_at.isoformat(),
                "store_name": "BAREKET",
                "files_types": "PRICE_FILE",
                "had_errors": False,
                "output_path": "/tmp/out.csv",
                "total_files": len(processed),
                "task_id": task_id,
            },
        ],
        "events": events,
    }


@pytest.fixture
def status_dirs(tmp_path):
    scraping = tmp_path / "scraping_status"
    converting = tmp_path / "converting_status"
    quality = tmp_path / "quality"
    scraping.mkdir()
    converting.mkdir()
    return {
        "scraping": str(scraping),
        "converting": str(converting),
        "quality": str(quality),
    }


def test_compute_scraper_quality_counts_saw_and_downloaded(status_dirs):
    started = datetime(2026, 1, 1, 10, 0, 0)
    bareket_file = DumpFolderNames["BAREKET"].value.lower()
    _write_json(
        os.path.join(status_dirs["scraping"], f"{bareket_file}.json"),
        _scraper_status(
            "task-1",
            started,
            saw_files=["Price123.xml", "Price456.xml", "Promo789.xml"],
            downloaded_files=["Price123.xml"],
            failed_downloads=[("Price456.xml", "timeout")],
        ),
    )

    result = compute_scraper_quality(status_dirs["scraping"], ["BAREKET", "WOLT"])

    assert len(result["iterations"]) == 1
    iteration = result["iterations"][0]
    bareket = iteration["scrapers"]["BAREKET"]
    assert bareket["saw"] == 3
    assert bareket["downloaded"] == 1
    assert bareket["no_data"] is False
    assert bareket["saw_not_downloaded"] == [
        {
            "file_name": "Price456",
            "reason": "download_failed",
            "error": "timeout",
        },
        {
            "file_name": "Promo789",
            "reason": "skipped_by_filter",
            "error": "not selected for download (filtered before collection)",
        },
    ]
    assert iteration["scrapers"]["WOLT"]["no_data"] is True
    assert iteration["scrapers"]["WOLT"]["saw_not_downloaded"] == []
    assert iteration["scrapers_with_no_data"] == 1


def test_compute_scraper_quality_skipped_by_limit(status_dirs):
    started = datetime(2026, 1, 1, 10, 0, 0)
    bareket_file = DumpFolderNames["BAREKET"].value.lower()
    _write_json(
        os.path.join(status_dirs["scraping"], f"{bareket_file}.json"),
        _scraper_status(
            "task-1",
            started,
            saw_files=["Price1.xml", "Price2.xml"],
            downloaded_files=["Price1.xml"],
            limit=1,
            saw_links={
                "Price2.xml": "http://example.com/Price2.xml.gz",
            },
        ),
    )

    result = compute_scraper_quality(status_dirs["scraping"], ["BAREKET"])

    skipped = result["iterations"][0]["scrapers"]["BAREKET"]["saw_not_downloaded"]
    assert skipped == [
        {
            "file_name": "Price2",
            "link": "http://example.com/Price2.xml.gz",
            "reason": "skipped_by_limit",
            "error": "not selected for download (limit=1, 1/2 files downloaded)",
        }
    ]


def test_compute_scraper_quality_two_iterations(status_dirs):
    bareket_file = DumpFolderNames["BAREKET"].value.lower()
    _write_json(
        os.path.join(status_dirs["scraping"], f"{bareket_file}.json"),
        {
            **_scraper_status(
                "task-1",
                datetime(2026, 1, 1, 8, 0, 0),
                saw_files=["Price1.xml"],
                downloaded_files=["Price1.xml"],
            ),
            "events": _scraper_status(
                "task-1",
                datetime(2026, 1, 1, 8, 0, 0),
                saw_files=["Price1.xml"],
                downloaded_files=["Price1.xml"],
            )["events"]
            + _scraper_status(
                "task-2",
                datetime(2026, 1, 1, 14, 0, 0),
                saw_files=["Price2.xml", "Price3.xml"],
                downloaded_files=[],
            )["events"],
            "global_status": [
                {
                    "task_id": "task-1",
                    "status": "started",
                    "system_timestamp": datetime(2026, 1, 1, 8, 0, 0).isoformat(),
                },
                {
                    "task_id": "task-2",
                    "status": "started",
                    "system_timestamp": datetime(2026, 1, 1, 14, 0, 0).isoformat(),
                },
            ],
            "verified_downloads": _scraper_status(
                "task-1",
                datetime(2026, 1, 1, 8, 0, 0),
                saw_files=["Price1.xml"],
                downloaded_files=["Price1.xml"],
            )["verified_downloads"],
        },
    )

    result = compute_scraper_quality(status_dirs["scraping"], ["BAREKET"])

    assert len(result["iterations"]) == 2
    assert result["iterations"][0]["scrapers"]["BAREKET"]["downloaded"] == 1
    assert result["iterations"][1]["scrapers"]["BAREKET"]["downloaded"] == 0
    assert result["iterations"][1]["scrapers"]["BAREKET"]["saw_not_downloaded"] == [
        {
            "file_name": "Price2",
            "reason": "skipped_by_filter",
            "error": "not selected for download (filtered before collection)",
        },
        {
            "file_name": "Price3",
            "reason": "skipped_by_filter",
            "error": "not selected for download (filtered before collection)",
        },
    ]
    assert result["iterations"][1]["scrapers_with_no_data"] == 1


def test_compute_parser_quality_zero_records_and_not_parsed(status_dirs):
    scrape_started = datetime(2026, 1, 1, 10, 0, 0)
    parse_started = datetime(2026, 1, 1, 10, 5, 0)
    bareket_file = DumpFolderNames["BAREKET"].value.lower()

    _write_json(
        os.path.join(status_dirs["scraping"], f"{bareket_file}.json"),
        _scraper_status(
            "scrape-task-1",
            scrape_started,
            saw_files=["Price123.xml", "Price456.xml"],
            downloaded_files=["Price123.xml", "Price456.xml"],
        ),
    )
    _write_json(
        os.path.join(status_dirs["converting"], "bareket_price_file.json"),
        _parser_status(
            "parse-task-1",
            parse_started,
            processed=[("Price123.xml", 0)],
        ),
    )

    result = compute_parser_quality(
        status_dirs["scraping"],
        status_dirs["converting"],
        ["BAREKET"],
        ["PRICE_FILE"],
    )

    assert len(result["iterations"]) == 1
    parser_metrics = result["iterations"][0]["parsers"]["bareket_price_file"]
    assert parser_metrics["zero_record_files"] == ["Price123"]
    assert parser_metrics["downloaded_not_parsed"] == ["Price456"]


def test_compute_pipeline_health_flags_unhealthy_parser():
    scraper_quality = {
        "iterations": [
            {
                "scrapers_with_no_data": 0,
                "scrapers": {
                    "BAREKET": {
                        "saw": 2,
                        "downloaded": 2,
                        "saw_not_downloaded": [],
                    }
                },
            }
        ]
    }
    parser_quality = {
        "iterations": [
            {
                "parsers": {
                    "bareket_price_file": {
                        "zero_record_files": ["Price123"],
                        "downloaded_not_parsed": ["Price456"],
                    }
                }
            }
        ]
    }

    health = compute_pipeline_health(
        scraper_quality,
        parser_quality,
        thresholds={
            "scraper_download_success_rate_min": 0.95,
            "parser_parse_success_rate_min": 0.98,
            "scraper_no_data_scrapers_max": 0,
        },
    )

    assert health["scraper"]["healthy"] is True
    assert health["parser"]["healthy"] is False
    assert health["parser"]["parse_success_rate"] == 0.0
    assert health["parser"]["below_threshold"] == ["parse_success_rate"]
    assert health["overall_healthy"] is False


def test_compute_pipeline_health_flags_unhealthy_scraper():
    scraper_quality = {
        "iterations": [
            {
                "scrapers_with_no_data": 1,
                "scrapers": {
                    "BAREKET": {
                        "saw": 10,
                        "downloaded": 8,
                        "saw_not_downloaded": [
                            {"reason": "download_failed"},
                            {"reason": "download_failed"},
                        ],
                    }
                },
            }
        ]
    }
    parser_quality = {"iterations": []}

    health = compute_pipeline_health(
        scraper_quality,
        parser_quality,
        thresholds={
            "scraper_download_success_rate_min": 0.95,
            "parser_parse_success_rate_min": 0.98,
            "scraper_no_data_scrapers_max": 0,
        },
    )

    assert health["scraper"]["download_success_rate"] == 0.8
    assert health["scraper"]["healthy"] is False
    assert "download_success_rate" in health["scraper"]["below_threshold"]
    assert health["overall_healthy"] is False


def test_write_quality_indicators_creates_files(status_dirs):
    bareket_file = DumpFolderNames["BAREKET"].value.lower()
    started = datetime(2026, 1, 1, 10, 0, 0)
    _write_json(
        os.path.join(status_dirs["scraping"], f"{bareket_file}.json"),
        _scraper_status("task-1", started, ["Price1.xml"], ["Price1.xml"]),
    )
    _write_json(
        os.path.join(status_dirs["converting"], "bareket_price_file.json"),
        _parser_status("parse-1", started, [("Price1.xml", 5)]),
    )

    write_quality_indicators(
        status_dirs["quality"],
        status_dirs["scraping"],
        status_dirs["converting"],
        ["BAREKET"],
        FileTypesFilters.all_types(),
    )

    scraper_path = os.path.join(status_dirs["quality"], SCRAPER_QUALITY_FILENAME)
    parser_path = os.path.join(status_dirs["quality"], PARSER_QUALITY_FILENAME)
    health_path = os.path.join(status_dirs["quality"], PIPELINE_HEALTH_FILENAME)
    assert os.path.isfile(scraper_path)
    assert os.path.isfile(parser_path)
    assert os.path.isfile(health_path)

    with open(scraper_path, encoding="utf-8") as handle:
        scraper_payload = json.load(handle)
    with open(parser_path, encoding="utf-8") as handle:
        parser_payload = json.load(handle)
    with open(health_path, encoding="utf-8") as handle:
        health_payload = json.load(handle)

    assert "computed_at" in scraper_payload
    assert "iterations" in parser_payload
    assert len(scraper_payload["iterations"]) >= 1
    assert "overall_healthy" in health_payload
    assert "scraper" in health_payload
    assert "parser" in health_payload


def test_write_quality_indicators_skips_missing_scraping_folder(tmp_path):
    quality = tmp_path / "quality"
    write_quality_indicators(
        str(quality),
        str(tmp_path / "missing_scraping"),
        str(tmp_path / "missing_converting"),
        ["BAREKET"],
        ["PRICE_FILE"],
    )
    assert not quality.exists()
