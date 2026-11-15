"""Scraper and parser quality hold one chain's status in memory at a time (#177).

The prod data-fetcher was OOM-killed (exit 137) while recomputing quality
indicators, because every chain's status JSON was loaded before any metric was
computed. These tests pin the per-chain behaviour.
"""

import gc
import json
import os
import tracemalloc
import weakref
from datetime import datetime

import pytest
from il_supermarket_scarper import DumpFolderNames, ScraperFactory

from managers.indicators import parser_quality, scraper_quality
from managers.quality_indicators import (
    compute_parser_quality,
    compute_scraper_quality,
)
from managers.tests.test_quality_indicators import (
    _parser_status,
    _scraper_status,
    _write_json,
)

FILE_TYPE = "PRICE_FILE"


def _chains(count):
    """``count`` scrapers with distinct status-file stems."""
    chains, stems = [], set()
    for name in ScraperFactory.all_scrapers_name():
        stem = DumpFolderNames[name].value.lower()
        if stem in stems:
            continue
        stems.add(stem)
        chains.append(name)
        if len(chains) == count:
            return chains
    raise AssertionError("not enough scrapers")


def _write_chains(tmp_path, chains, files_per_chain):
    scraping = tmp_path / "scraping_status"
    converting = tmp_path / "converting_status"
    started = datetime(2026, 1, 1, 10, 0, 0)
    for index, chain in enumerate(chains):
        names = [f"Price{index:03d}-{n:05d}.xml" for n in range(files_per_chain)]
        stem = DumpFolderNames[chain].value.lower()
        _write_json(
            str(scraping / f"{stem}.json"),
            _scraper_status(
                f"scrape-{index}",
                started,
                saw_files=names,
                downloaded_files=names[: files_per_chain // 2],
            ),
        )
        _write_json(
            str(converting / (f"{chain}_{FILE_TYPE}".lower() + ".json")),
            _parser_status(
                f"parse-{index}",
                started,
                processed=[(name, n % 3) for n, name in enumerate(names[:10])],
            ),
        )
    return str(scraping), str(converting)


class _TrackedStatus(dict):
    """A status JSON dict that can be weak-referenced to count live copies."""


@pytest.fixture
def live_status_counter(monkeypatch):
    """Patch the status loaders; record the most status dicts alive at one load."""
    refs = []
    peak = {"value": 0}

    def load(path):
        if not os.path.isfile(path):
            return None
        with open(path, "r", encoding="utf-8") as handle:
            raw = _TrackedStatus(json.load(handle))
        gc.collect()
        refs.append(weakref.ref(raw))
        alive = sum(1 for ref in refs if ref() is not None)
        peak["value"] = max(peak["value"], alive)
        return raw

    monkeypatch.setattr(scraper_quality, "_load_status_json", load)
    monkeypatch.setattr(parser_quality, "_load_status_json", load)
    return peak


def test_scraper_quality_holds_one_status_file_at_a_time(tmp_path, live_status_counter):
    chains = _chains(6)
    scraping, _ = _write_chains(tmp_path, chains, files_per_chain=20)

    result = compute_scraper_quality(scraping, chains)

    assert set(result["iterations"][0]["scrapers"]) == set(chains)
    assert live_status_counter["value"] == 1


def test_parser_quality_holds_one_chain_at_a_time(tmp_path, live_status_counter):
    chains = _chains(6)
    scraping, converting = _write_chains(tmp_path, chains, files_per_chain=20)

    result = compute_parser_quality(scraping, converting, chains, [FILE_TYPE])

    assert len(result["iterations"][0]["parsers"]) == len(chains)
    # The chain's scraper status plus the one parser file being reduced.
    assert live_status_counter["value"] <= 2


def test_multi_chain_result_matches_each_chain_alone(tmp_path):
    chains = _chains(4)
    scraping, converting = _write_chains(tmp_path, chains, files_per_chain=15)

    together_scrape = compute_scraper_quality(scraping, chains)
    together_parse = compute_parser_quality(scraping, converting, chains, [FILE_TYPE])

    for chain in chains:
        alone_scrape = compute_scraper_quality(scraping, [chain])
        alone_parse = compute_parser_quality(scraping, converting, [chain], [FILE_TYPE])
        assert (
            together_scrape["iterations"][0]["scrapers"][chain]
            == alone_scrape["iterations"][0]["scrapers"][chain]
        )
        key = f"{chain}_{FILE_TYPE}".lower()
        assert (
            together_parse["iterations"][0]["parsers"][key]
            == alone_parse["iterations"][0]["parsers"][key]
        )
    assert together_scrape["iterations"][0]["started_at"] == min(
        compute_scraper_quality(scraping, [c])["iterations"][0]["started_at"]
        for c in chains
    )


def _traced_peak(fn):
    gc.collect()
    tracemalloc.start()
    try:
        fn()
        return tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


@pytest.mark.parametrize("compute", ["scraper", "parser"])
def test_peak_memory_does_not_grow_with_chain_count(tmp_path, compute):
    chains = _chains(8)
    scraping, converting = _write_chains(tmp_path, chains, files_per_chain=1500)

    def run(selected):
        if compute == "scraper":
            return lambda: compute_scraper_quality(scraping, selected)
        return lambda: compute_parser_quality(
            scraping, converting, selected, [FILE_TYPE]
        )

    one = _traced_peak(run(chains[:1]))
    eight = _traced_peak(run(chains))

    # Holding every chain at once would be ~8x; per chain stays near 1x plus the
    # small per-chain metrics that are kept for the output.
    assert eight < 2.5 * one, (one, eight)
