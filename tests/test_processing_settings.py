"""Scraper and parser pool sizes read from the environment (#141)."""

import pytest

from main import processing_settings_from_env


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("NUM_OF_PROCESSES", "NUM_OF_PARSING_PROCESSES"):
        monkeypatch.delenv(name, raising=False)


def test_defaults_keep_todays_behaviour():
    assert processing_settings_from_env() == {
        "number_of_scraping_processes": 5,
        "number_of_parseing_processs": 5,
    }


def test_parsing_follows_scraping_when_unset_or_empty(monkeypatch):
    # docker compose passes an unset ${VAR:-} as an empty string
    monkeypatch.setenv("NUM_OF_PROCESSES", "4")
    monkeypatch.setenv("NUM_OF_PARSING_PROCESSES", "")
    assert processing_settings_from_env()["number_of_parseing_processs"] == 4


def test_parsing_processes_reach_their_setting(monkeypatch):
    monkeypatch.setenv("NUM_OF_PROCESSES", "5")
    monkeypatch.setenv("NUM_OF_PARSING_PROCESSES", "3")
    assert processing_settings_from_env() == {
        "number_of_scraping_processes": 5,
        "number_of_parseing_processs": 3,
    }


@pytest.mark.parametrize("value", ["0", "-1", "ten"])
def test_invalid_parsing_processes_are_rejected(monkeypatch, value):
    monkeypatch.setenv("NUM_OF_PARSING_PROCESSES", value)
    with pytest.raises(ValueError, match="NUM_OF_PARSING_PROCESSES"):
        processing_settings_from_env()
