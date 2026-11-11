"""Indicators on what reached the parser and what it parsed."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Set

from managers.indicators.base import Indicator
from managers.indicators.status_files import (
    dump_stem,
    load_status_json,
    normalize_filename,
)

PARSER_SAW_STATUSES = frozenset({"processed", "skipped", "failed"})


class ParseSuccessRate(Indicator):
    """1 - (zero-record + unparsed files) / downloads, over the status window."""

    name = "parse_success_rate"
    section = "parser"
    threshold_key = "parser_parse_success_rate_min"
    env_var = "QUALITY_PARSER_MIN_PARSE_SUCCESS_RATE"
    op = "min"

    def section_value(self, section):
        return section.get("parse_success_rate")

    @staticmethod
    def compute(problem_files: int, downloaded: int) -> float:
        """1 - problem files / downloads (1.0 with nothing downloaded)."""
        baseline = downloaded if downloaded > 0 else problem_files
        return round(1.0 - problem_files / baseline, 4) if baseline > 0 else 1.0


class UnparsedDownloads(Indicator):
    """Downloaded files the parser has no record of, for the whole day."""

    name = "unparsed_downloads"
    section = "unparsed_downloads"
    threshold_key = "unparsed_downloads_max"
    env_var = "QUALITY_MAX_UNPARSED_DOWNLOADS"
    op = "max"

    def section_value(self, section):
        return section.get("total_files")

    @staticmethod
    def compute(
        scraping_status_folder: str,
        converting_status_folder: str,
        enabled_scrapers: List[str],
        enabled_file_types: List[str],
    ) -> Dict[str, Any]:
        """Files the scraper picked that never reached the parser, for the day.

        Picked is every ``collected`` event. A file whose every download
        attempt failed is left out: it is already counted in the download
        success rate, and the parser was never meant to see it. Parser-saw is
        every processed, skipped, or failed event. All tasks in the status
        folders are one set. Names are normalized.
        """
        by_chain: List[Dict[str, Any]] = []
        total = 0
        for scraper in enabled_scrapers:
            stem = dump_stem(scraper)
            scraper_raw = load_status_json(
                os.path.join(scraping_status_folder, f"{stem}.json")
            )
            selected: Set[str] = set()
            download_ok: Set[str] = set()
            download_failed: Set[str] = set()
            for event in (scraper_raw or {}).get("events") or []:
                if not event.get("file_name"):
                    continue
                name = normalize_filename(event["file_name"])
                if event.get("status") == "collected":
                    selected.add(name)
                elif event.get("status") == "downloaded":
                    if event.get("downloaded_successfully") and event.get(
                        "extracted_successfully"
                    ):
                        download_ok.add(name)
                    else:
                        download_failed.add(name)
            selected -= download_failed - download_ok

            parser_saw: Set[str] = set()
            for file_type in enabled_file_types:
                parser_raw = load_status_json(
                    os.path.join(
                        converting_status_folder,
                        f"{scraper}_{file_type}".lower() + ".json",
                    )
                )
                for event in (parser_raw or {}).get("events") or []:
                    if event.get("status") not in PARSER_SAW_STATUSES:
                        continue
                    if event.get("file_name"):
                        parser_saw.add(normalize_filename(event["file_name"]))

            gap = sorted(selected - parser_saw)
            if not gap:
                continue
            total += len(gap)
            by_chain.append(
                {
                    "chain": stem,
                    "enum": scraper,
                    "selected": len(selected),
                    "parser_saw": len(parser_saw),
                    "file_count": len(gap),
                    "files": gap,
                }
            )

        by_chain.sort(key=lambda row: (-row["file_count"], row["chain"]))
        return {"total_files": total, "by_chain": by_chain}


def parser_section(
    parser_quality: Dict[str, Any], downloaded_baseline: int
) -> Dict[str, Any]:
    """The ``parser`` section of ``pipeline_health.json``."""
    zero_record: Set[str] = set()
    unparsed: Set[str] = set()

    for iteration in parser_quality.get("iterations", []):
        for parser_metrics in iteration.get("parsers", {}).values():
            zero_record.update(parser_metrics.get("zero_record_files", []))
            unparsed.update(parser_metrics.get("downloaded_not_parsed", []))

    problem_files = zero_record | unparsed

    return {
        "total_zero_record_files": len(zero_record),
        "total_downloaded_not_parsed": len(unparsed),
        "total_problem_files": len(problem_files),
        "downloaded_baseline": downloaded_baseline,
        "parse_success_rate": ParseSuccessRate.compute(
            len(problem_files), downloaded_baseline
        ),
    }
