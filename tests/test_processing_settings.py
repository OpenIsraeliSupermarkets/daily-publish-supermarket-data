"""Process counts and queue caps read from the environment (#141)."""

from unittest.mock import MagicMock, patch

import pytest

from main import processing_settings_from_env
from publishers.base_publisher import BaseSupermarketDataPublisher
from publishers.dag_simulator import SupermarketDataPublisher

KNOBS = (
    "NUM_OF_PROCESSES",
    "NUM_OF_PARSING_PROCESSES",
    "SCRAPE_QUEUE_MAX_FILES",
    "PARSE_QUEUE_MAX_FILES",
    "PARSED_ROWS_QUEUE_MAX",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in KNOBS:
        monkeypatch.delenv(name, raising=False)


def test_defaults_keep_todays_behaviour():
    assert processing_settings_from_env() == {
        "number_of_scraping_processes": 5,
        "number_of_parseing_processs": 5,
        "scrape_queue_max_files": 0,
        "parse_queue_max_files": 0,
        "parsed_rows_queue_max": 0,
    }


def test_empty_values_are_defaults(monkeypatch):
    # docker compose passes an unset ${VAR:-} as an empty string
    for name in KNOBS[1:]:
        monkeypatch.setenv(name, "")
    monkeypatch.setenv("NUM_OF_PROCESSES", "4")
    settings = processing_settings_from_env()
    assert settings["number_of_parseing_processs"] == 4
    assert settings["scrape_queue_max_files"] == 0


@pytest.mark.parametrize(
    "env_name,setting",
    [
        ("NUM_OF_PROCESSES", "number_of_scraping_processes"),
        ("NUM_OF_PARSING_PROCESSES", "number_of_parseing_processs"),
        ("SCRAPE_QUEUE_MAX_FILES", "scrape_queue_max_files"),
        ("PARSE_QUEUE_MAX_FILES", "parse_queue_max_files"),
        ("PARSED_ROWS_QUEUE_MAX", "parsed_rows_queue_max"),
    ],
)
def test_each_knob_reaches_its_setting(monkeypatch, env_name, setting):
    monkeypatch.setenv(env_name, "3")
    assert processing_settings_from_env()[setting] == 3


@pytest.mark.parametrize("value", ["1", "2"])
def test_scrape_cap_below_three_is_rejected(monkeypatch, value):
    # a smaller cap deadlocks on the extra end-of-stream markers
    monkeypatch.setenv("SCRAPE_QUEUE_MAX_FILES", value)
    with pytest.raises(ValueError, match="SCRAPE_QUEUE_MAX_FILES"):
        processing_settings_from_env()


@pytest.mark.parametrize("value", ["-1", "ten"])
@pytest.mark.parametrize("env_name", KNOBS[1:])
def test_invalid_caps_are_rejected(monkeypatch, env_name, value):
    monkeypatch.setenv(env_name, value)
    with pytest.raises(ValueError, match=env_name):
        processing_settings_from_env()


def test_dag_publisher_forwards_caps(tmp_path):
    publisher = SupermarketDataPublisher(
        app_folder=str(tmp_path),
        scrape_queue_max_files=7,
        parse_queue_max_files=8,
        parsed_rows_queue_max=9,
    )
    assert publisher.scrape_queue_max_files == 7
    assert publisher.parse_queue_max_files == 8
    assert publisher.parsed_rows_queue_max == 9


def test_caps_reach_the_scraper_and_parser_tasks(tmp_path):
    publisher = BaseSupermarketDataPublisher(
        app_folder=str(tmp_path),
        enabled_scrapers=["BAREKET"],
        enabled_file_types=["STORE_FILE"],
        scrape_queue_max_files=7,
        parse_queue_max_files=8,
        parsed_rows_queue_max=9,
        short_term_db_target=MagicMock(),
    )
    with patch("publishers.base_publisher.ScarpingTask") as scraper, patch(
        "publishers.base_publisher.ConvertingTask"
    ) as converter, patch("publishers.base_publisher.CacheManager"), patch(
        "publishers.base_publisher.StreamPushManager"
    ), patch(
        "publishers.base_publisher.write_quality_indicators"
    ):
        publisher._execute_scraping()

    assert scraper.call_args.kwargs["output_configuration"]["max_queue_size"] == 7
    assert converter.call_args.kwargs["max_queue_size"] == 8
    assert converter.call_args.kwargs["max_output_queue_size"] == 9
