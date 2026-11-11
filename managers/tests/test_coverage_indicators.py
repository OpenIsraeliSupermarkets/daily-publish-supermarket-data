"""End-to-end coverage and download lag from raw status JSON."""

import json
import os

import pandas as pd
import pytest

from il_supermarket_scarper import DumpFolderNames

from managers.coverage_indicators import (
    compute_download_published_lag,
    compute_end_to_end_coverage,
    file_name_timestamp,
)
from managers.quality_indicators import assess_published_health, compute_pipeline_health

STEM = DumpFolderNames["BAREKET"].value.lower()
TYPES = ["PRICE_FILE", "PROMO_FILE"]


def _write(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _event(status, name, task="scrape-1", at="2026-11-10 09:10:00+02:00", **extra):
    return {
        "status": status,
        "file_name": name,
        "task_id": task,
        "system_timestamp": at,
        **extra,
    }


def _downloaded(name, ok=True, at="2026-11-10 09:15:00+02:00", decision="created"):
    return _event(
        "downloaded",
        name,
        at=at,
        downloaded_successfully=ok,
        extracted_successfully=ok,
        save_decision=decision if ok else None,
    )


@pytest.fixture
def dirs(tmp_path):
    out = {name: tmp_path / name for name in ("scraping", "converting", "outputs")}
    for path in out.values():
        path.mkdir()
    return {k: str(v) for k, v in out.items()}


def _scenario(dirs):
    names = {
        "published": "Price7290875100001-001-001-20261110-090000.gz",
        "empty": "Price7290875100001-001-002-20261110-090000.gz",
        "not_in_csv": "Price7290875100001-001-003-20261110-090000.gz",
        "failed_download": "Price7290875100001-001-004-20261110-090000.gz",
        "not_parsed": "Price7290875100001-001-005-20261110-090000.gz",
        "earlier_day": "Price7290875100001-001-006-20261101-090000.gz",
        "null_name": "NULLPrice7290875100001-001-007-20261110-090000.gz",
        "reverified": "Price7290875100001-001-008-20261110-090000.gz",
        "pending": "Price7290875100001-001-009-20261110-090000.gz",
        "unexplained": "Price7290875100001-001-010-20261110-090000.gz",
        "other_type": "Stores7290875100001-000-20261110-090000.gz",
    }
    events = [_event("saw", name) for name in names.values()]
    for key in ("published", "empty", "not_in_csv", "not_parsed", "reverified"):
        events.append(_event("collected", names[key]))
        events.append(
            _downloaded(
                names[key], decision="rewrote_same" if key == "reverified" else "created"
            )
        )
    events.append(_event("collected", names["failed_download"]))
    events.append(_downloaded(names["failed_download"], ok=False))
    events.append(_event("collected", names["pending"]))
    events.append(_downloaded(names["pending"], at="2026-11-10 11:00:00+02:00"))
    _write(
        os.path.join(dirs["scraping"], f"{STEM}.json"),
        {
            "global_status": [
                {
                    "status": "started",
                    "task_id": "scrape-1",
                    "when_date": "2026-11-10 08:13:00+02:00",
                    "files_requested": TYPES + ["STORE_FILE"],
                    "limit": None,
                }
            ],
            "events": events,
            "verified_downloads": [],
        },
    )

    def processed(key, rows):
        return {
            "status": "processed",
            "file_name": names[key].replace(".gz", ".xml"),
            "row_count": rows,
            "task_id": "parse-1",
        }

    _write(
        os.path.join(dirs["converting"], "bareket_price_file.json"),
        {
            "global_status": [
                {
                    "status": "started",
                    "task_id": "parse-1",
                    "system_timestamp": "2026-11-10 10:00:00+02:00",
                }
            ],
            "events": [
                processed("published", 10),
                processed("empty", 0),
                processed("not_in_csv", 4),
            ],
        },
    )
    pd.DataFrame(
        {"file_name": [names["published"].replace(".gz", ".xml")] * 2, "price": [1, 2]}
    ).to_csv(os.path.join(dirs["outputs"], "price_file_bareket.csv"), index=False)
    return names


def test_coverage_buckets_every_seen_file_by_where_it_stopped(dirs):
    _scenario(dirs)

    result = compute_end_to_end_coverage(
        dirs["scraping"], dirs["converting"], dirs["outputs"], ["BAREKET"], TYPES
    )

    assert result["buckets"] == {
        "download_failed": 1,
        "earlier_day": 1,
        "empty_at_source": 1,
        "not_in_csv": 1,
        "not_parsed": 1,
        "pending": 1,
        "published": 1,
        "reverified": 1,
        "skipped_null_name": 1,
        "skipped_unexplained": 1,
    }
    # The store file is not an enabled type, so it is not counted at all.
    assert result["seen"] == 10
    # Earlier-day, re-verified and pending files are not today's data yet.
    assert result["expected"] == 7
    # Published plus the file whose source was empty.
    assert result["covered"] == 2
    assert result["lost"] == 5
    assert result["coverage_rate"] == round(2 / 7, 4)
    chain = result["by_chain"][0]
    assert chain["lost_files_sample"]["not_in_csv"] == [
        "Price7290875100001-001-003-20261110-090000"
    ]


def test_coverage_check_fails_on_any_not_in_csv_file(dirs):
    _scenario(dirs)
    coverage = compute_end_to_end_coverage(
        dirs["scraping"], dirs["converting"], dirs["outputs"], ["BAREKET"], TYPES
    )
    thresholds = {
        "scraper_download_success_rate_min": 0.95,
        "parser_parse_success_rate_min": 0.98,
        "scraper_no_data_scrapers_max": 0,
        "unparsed_downloads_max": 0,
        "end_to_end_coverage_rate_min": 0.0,
        "not_in_csv_max": 0,
    }
    health = compute_pipeline_health(
        {"iterations": []},
        {"iterations": []},
        thresholds=thresholds,
        unparsed_downloads={"total_files": 0, "by_chain": []},
        end_to_end_coverage=coverage,
    )

    assert health["end_to_end_coverage"]["below_threshold"] == ["not_in_csv"]
    assert health["overall_healthy"] is False
    failed = [c["name"] for c in assess_published_health(health, thresholds)["checks"] if not c["ok"]]
    assert "not_in_csv" in failed
    assert "end_to_end_coverage_rate" not in failed


def test_assess_fails_when_coverage_is_missing():
    checks = assess_published_health({"scraper": {}, "parser": {}})["checks"]
    coverage = next(c for c in checks if c["name"] == "end_to_end_coverage_rate")
    assert coverage["value"] is None and coverage["ok"] is False


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Promo7290875100001-002-008-20261110-080655", "2026-11-10 08:06:55"),
        ("Price7290785400000-002-202611100900", "2026-11-10 09:00:00"),
        ("Stores7290058249350-000-20261109", "2026-11-09 00:00:00"),
        ("Stores7290058249350", None),
    ],
)
def test_file_name_timestamp(name, expected):
    parsed = file_name_timestamp(name)
    if expected is None:
        assert parsed is None
    else:
        assert parsed.strftime("%Y-%m-%d %H:%M:%S") == expected
        assert parsed.utcoffset() is not None


def test_download_published_lag_per_chain(dirs):
    _write(
        os.path.join(dirs["scraping"], f"{STEM}.json"),
        {
            "global_status": [],
            "events": [
                _event("saw", "Price7290875100001-001-001-20261110-090000.gz"),
                _downloaded(
                    "Price7290875100001-001-001-20261110-090000.gz",
                    at="2026-11-10 10:00:00+02:00",
                ),
                _downloaded(
                    "Price7290875100001-001-002-20261110-090000.gz",
                    at="2026-11-10 12:00:00+02:00",
                ),
                _downloaded("Stores7290875100001.gz"),
                _downloaded(
                    "Price7290875100001-001-003-20261110-090000.gz", ok=False
                ),
            ],
            "verified_downloads": [],
        },
    )

    lag = compute_download_published_lag(dirs["scraping"], ["BAREKET"])

    assert lag["files"] == 2
    assert lag["p50_hours"] == 2.0
    assert lag["max_hours"] == 3.0
    assert lag["no_timestamp"] == 1
    assert lag["by_chain"][0]["chain"] == STEM
