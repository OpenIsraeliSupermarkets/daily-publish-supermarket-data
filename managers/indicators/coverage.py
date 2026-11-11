"""End-to-end coverage: did every file the sites listed reach the CSVs?

We do not control what the chains publish or when. The only view of the
source is what the scrapers *saw* in each listing. Every seen file is followed
to the CSV rows we publish, and put in the bucket of where it stopped.
"""

from __future__ import annotations

import os
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Set

import pandas as pd

from il_supermarket_scarper import FileTypesFilters

from managers.indicators.base import Indicator
from managers.indicators.file_names import file_name_date
from managers.indicators.status_files import (
    dump_stem,
    load_status_json,
    normalize_filename,
    queued_for_parse,
)
from managers.indicators.views import ParserView, ScraperView

PUBLISHED = "published"
EMPTY_AT_SOURCE = "empty_at_source"
EARLIER_DAY = "earlier_day"
REVERIFIED = "reverified"
PENDING = "pending"
SKIPPED_NULL_NAME = "skipped_null_name"
SKIPPED_TYPE_NOT_REQUESTED = "skipped_type_not_requested"
SKIPPED_BY_LIMIT = "skipped_by_limit"
SKIPPED_UNEXPLAINED = "skipped_unexplained"
DOWNLOAD_FAILED = "download_failed"
NOT_PARSED = "not_parsed"
PARSE_FAILED = "parse_failed"
NOT_IN_CSV = "not_in_csv"

# Left out of the rate: not today's data, or still on its way.
NOT_EXPECTED = frozenset({EARLIER_DAY, REVERIFIED, PENDING})
COVERED = frozenset({PUBLISHED, EMPTY_AT_SOURCE})
LOST = (
    SKIPPED_NULL_NAME,
    SKIPPED_TYPE_NOT_REQUESTED,
    SKIPPED_BY_LIMIT,
    SKIPPED_UNEXPLAINED,
    DOWNLOAD_FAILED,
    NOT_PARSED,
    PARSE_FAILED,
    NOT_IN_CSV,
)
SAMPLE_FILES = 20


def published_file_names(outputs_folder: str) -> Set[str]:
    """Normalized source file names that have rows in the output CSVs.

    Reads only the ``file_name`` column of every CSV under ``outputs_folder``.
    """
    names: Set[str] = set()
    if not outputs_folder or not os.path.isdir(outputs_folder):
        return names
    for root, _, files in os.walk(outputs_folder):
        for csv_name in files:
            if not csv_name.endswith(".csv"):
                continue
            path = os.path.join(root, csv_name)
            try:
                header = pd.read_csv(path, nrows=0)
            except (pd.errors.EmptyDataError, pd.errors.ParserError, OSError):
                continue
            if "file_name" not in header.columns:
                continue
            for chunk in pd.read_csv(
                path, usecols=["file_name"], dtype=str, chunksize=500_000
            ):
                names.update(
                    normalize_filename(os.path.basename(value))
                    for value in chunk["file_name"].dropna().unique()
                )
    return names


class SeenFileClassifier:
    """Puts every seen file of an enabled type in the bucket where it stopped."""

    def __init__(
        self,
        scraper: ScraperView,
        parser: ParserView,
        published: Set[str],
        enabled_file_types: Iterable[str],
    ):
        self.scraper = scraper
        self.parser = parser
        self.published = published
        self.enabled = set(enabled_file_types)

    def classify(self) -> Dict[str, str]:
        """Bucket per normalized file name."""
        buckets: Dict[str, str] = {}
        for name in self.scraper.first_seen:
            original = self.scraper.original_name(name)
            detected = FileTypesFilters.get_type_from_file(original)
            file_type = detected.name if detected is not None else None
            if file_type is not None and file_type not in self.enabled:
                continue
            buckets[name] = self.bucket(name, original, file_type)
        return buckets

    def bucket(  # pylint: disable=too-many-return-statements
        self, name: str, original: str, file_type: Optional[str]
    ) -> str:
        """Where one seen file stopped."""
        if name in self.published:
            return PUBLISHED
        if name in self.parser.zero_rows:
            return EMPTY_AT_SOURCE
        if name in self.parser.with_rows:
            return NOT_IN_CSV
        if name not in self.scraper.download_ok:
            if name in self.scraper.download_failed:
                return DOWNLOAD_FAILED
            return self.skip_reason(name, original, file_type)
        recorded = self.scraper.decisions.get(name) or [None]
        if not any(queued_for_parse(decision) for decision in recorded):
            return REVERIFIED
        if name in self.parser.failed:
            return PARSE_FAILED
        last_start = self.parser.last_start
        if last_start is None or self.scraper.download_ok[name] > last_start:
            return PENDING
        return NOT_PARSED

    def skip_reason(self, name: str, original: str, file_type: Optional[str]) -> str:
        """Why a seen file was never downloaded."""
        if original.upper().startswith("NULL"):
            return SKIPPED_NULL_NAME
        tasks = self.scraper.tasks_that_saw(name)
        file_day = file_name_date(original)
        if (
            tasks
            and file_day
            and all(task.day and task.day != file_day for task in tasks)
        ):
            return EARLIER_DAY
        if (
            tasks
            and file_type
            and all(
                task.files_requested and file_type not in task.files_requested
                for task in tasks
            )
        ):
            return SKIPPED_TYPE_NOT_REQUESTED
        if tasks and any(task.limit for task in tasks):
            return SKIPPED_BY_LIMIT
        return SKIPPED_UNEXPLAINED


def _rate(covered: int, expected: int) -> float:
    return round(covered / expected, 4) if expected else 1.0


class EndToEndCoverage:
    """Computes the ``end_to_end_coverage`` section of ``pipeline_health.json``.

    Expected = seen, minus files from an earlier day, re-verified copies, and
    files downloaded after the last parse started. Covered = files with rows
    in a CSV, plus files that parsed to zero rows (the source was empty).
    """

    section = "end_to_end_coverage"

    @staticmethod
    def compute(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        scraping_status_folder: str,
        converting_status_folder: str,
        outputs_folder: str,
        enabled_scrapers: List[str],
        enabled_file_types: List[str],
        published: Optional[Set[str]] = None,
    ) -> Dict[str, Any]:
        """Coverage overall and per chain, with a sample of lost files."""
        if published is None:
            published = published_file_names(outputs_folder)
        totals: Counter = Counter()
        by_chain: List[Dict[str, Any]] = []
        for scraper in enabled_scrapers:
            stem = dump_stem(scraper)
            raw = load_status_json(os.path.join(scraping_status_folder, f"{stem}.json"))
            if not raw:
                continue
            buckets = SeenFileClassifier(
                ScraperView.from_status(raw),
                ParserView.from_folder(
                    converting_status_folder, scraper, enabled_file_types
                ),
                published,
                enabled_file_types,
            ).classify()
            counts = Counter(buckets.values())
            totals.update(counts)
            expected = sum(v for k, v in counts.items() if k not in NOT_EXPECTED)
            covered = sum(counts[k] for k in COVERED)
            by_chain.append(
                {
                    "chain": stem,
                    "enum": scraper,
                    "seen": len(buckets),
                    "expected": expected,
                    "covered": covered,
                    "coverage_rate": _rate(covered, expected),
                    "buckets": dict(counts),
                    "lost_files_sample": {
                        bucket: sorted(n for n, b in buckets.items() if b == bucket)[
                            :SAMPLE_FILES
                        ]
                        for bucket in LOST
                        if counts[bucket]
                    },
                }
            )

        expected = sum(v for k, v in totals.items() if k not in NOT_EXPECTED)
        covered = sum(totals[k] for k in COVERED)
        by_chain.sort(key=lambda row: (row["coverage_rate"], row["chain"]))
        return {
            "seen": sum(totals.values()),
            "expected": expected,
            "covered": covered,
            "lost": expected - covered,
            "coverage_rate": _rate(covered, expected),
            "buckets": {bucket: totals[bucket] for bucket in sorted(totals)},
            "by_chain": by_chain,
        }


class EndToEndCoverageRate(Indicator):
    """Share of today's seen files whose data reached the output CSVs."""

    name = "end_to_end_coverage_rate"
    section = EndToEndCoverage.section
    threshold_key = "end_to_end_coverage_rate_min"
    env_var = "QUALITY_MIN_END_TO_END_COVERAGE"
    op = "min"

    def section_value(self, section):
        return section.get("coverage_rate")


class NotInCsv(Indicator):
    """Files parsed with rows but missing from the output CSVs."""

    name = "not_in_csv"
    section = EndToEndCoverage.section
    threshold_key = "not_in_csv_max"
    env_var = "QUALITY_MAX_NOT_IN_CSV"
    op = "max"

    def section_value(self, section):
        return (section.get("buckets") or {}).get(NOT_IN_CSV, 0)
