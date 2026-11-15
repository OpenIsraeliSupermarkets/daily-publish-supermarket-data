"""The quality-indicator recompute logs time and memory per step (#180)."""

import os
from datetime import datetime

import pandas as pd
import pytest
from il_supermarket_scarper import DumpFolderNames

from managers.indicators.coverage import published_file_names
from managers.quality_indicators import write_quality_indicators
from managers.tests.test_quality_indicators import (
    _parser_status,
    _scraper_status,
    _write_json,
)
from utils import Logger
from utils.memory import memory_mb


@pytest.fixture
def info_lines(monkeypatch):
    """Every Logger.info message, formatted."""
    lines = []
    monkeypatch.setattr(
        Logger,
        "info",
        staticmethod(lambda msg, *args, **kwargs: lines.append(msg % args)),
    )
    return lines


def test_memory_mb_reports_a_positive_peak():
    rss, peak = memory_mb()
    assert peak > 0
    assert rss is None or rss > 0


def test_each_step_logs_time_and_memory(tmp_path, info_lines):
    scraping = tmp_path / "scraping_status"
    converting = tmp_path / "converting_status"
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    started = datetime(2026, 1, 1, 10, 0, 0)
    stem = DumpFolderNames["BAREKET"].value.lower()
    _write_json(
        str(scraping / f"{stem}.json"),
        _scraper_status("task-1", started, ["Price1.xml"], ["Price1.xml"]),
    )
    _write_json(
        str(converting / "bareket_price_file.json"),
        _parser_status("parse-1", started, [("Price1.xml", 5)]),
    )
    pd.DataFrame({"file_name": ["Price1.xml"]}).to_csv(
        outputs / "price_file_bareket.csv", index=False
    )

    write_quality_indicators(
        str(tmp_path / "quality"),
        str(scraping),
        str(converting),
        ["BAREKET"],
        ["PRICE_FILE"],
        outputs_folder=str(outputs),
    )

    steps = [line for line in info_lines if line.startswith("Step quality/")]
    assert [line.split(":")[0] for line in steps] == [
        "Step quality/scraper_quality",
        "Step quality/parser_quality",
        "Step quality/unparsed_downloads",
        "Step quality/end_to_end_coverage",
        "Step quality/download_published_lag",
        "Step quality/pipeline_health",
        "Step quality/write",
    ]
    assert all("peak=" in line and "rss=" in line for line in steps)


def test_coverage_scan_logs_each_csv_before_reading_it(tmp_path, info_lines):
    pd.DataFrame({"file_name": ["a/Price1.xml", "a/Price2.xml"]}).to_csv(
        tmp_path / "price_file_x.csv", index=False
    )
    pd.DataFrame({"other": [1]}).to_csv(tmp_path / "no_file_name.csv", index=False)

    names = published_file_names(str(tmp_path))

    assert names == {"Price1", "Price2"}
    reading = [line for line in info_lines if line.startswith("Coverage scan: reading")]
    assert sorted(line.split()[3] for line in reading) == [
        "no_file_name.csv",
        "price_file_x.csv",
    ]
    assert any(
        line.startswith("Coverage scan: 1 CSVs, 2 file names") for line in info_lines
    )
