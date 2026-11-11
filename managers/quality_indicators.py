"""Compute scrape/parse quality KPIs from status JSON logs."""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple


from il_supermarket_scarper import DumpFolderNames, FileTypesFilters

from managers.indicators import (
    DownloadPublishedLag,
    EndToEndCoverage,
    UnparsedDownloads,
    assess,
    failures_by_section,
    load_thresholds,
)
from managers.indicators.parser_quality import ParserQuality
from managers.indicators.parsing import parser_section
from managers.indicators.scraper_quality import ScraperQuality
from managers.indicators.scraping import scraper_section
from managers.indicators.status_files import (
    TZ,
    dump_stem,
    load_status_json,
    normalize_filename,
    queued_for_parse,
)
from utils import Logger, now

SCRAPER_QUALITY_FILENAME = "scraper_quality.json"
PARSER_QUALITY_FILENAME = "parser_quality.json"
PIPELINE_HEALTH_FILENAME = "pipeline_health.json"
KAGGLE_QUALITY_JSON_FILES = frozenset(
    {
        SCRAPER_QUALITY_FILENAME,
        PARSER_QUALITY_FILENAME,
        PIPELINE_HEALTH_FILENAME,
    }
)
_TZ = TZ
_dump_stem = dump_stem
_load_status_json = load_status_json
_save_decision_queued_for_parse = queued_for_parse
compute_scraper_quality = ScraperQuality.compute
compute_parser_quality = ParserQuality.compute


_DUMP_FILENAME_RE = re.compile(
    r"^(?P<kind>PriceFull|PromoFull|Price|Promo|Stores)"
    r"(?P<chain>\d+)"
    r"(?:-(?P<rest>.*))?$",
    re.IGNORECASE,
)


def generalize_filename_patterns(
    filenames: Iterable[str],
    example_limit: int = 5,
) -> List[Dict[str, Any]]:
    """Collapse dump filenames into joined patterns plus example names.

    Israeli dump names look like ``Price7290027600007-006-611-20260831-220000``.
    Constant hyphen fields stay literal; varying fields become ``*``.
    """
    grouped: Dict[Tuple[str, str, int], List[Tuple[str, str, List[str]]]] = defaultdict(
        list
    )
    leftovers: List[str] = []
    seen: Set[str] = set()
    ordered: List[str] = []
    for raw in filenames:
        if not raw:
            continue
        name = normalize_filename(str(raw))
        if name in seen:
            continue
        seen.add(name)
        ordered.append(name)

    for name in ordered:
        match = _DUMP_FILENAME_RE.match(name)
        if not match:
            leftovers.append(name)
            continue
        kind = match.group("kind")
        chain = match.group("chain")
        rest = match.group("rest") or ""
        parts = rest.split("-") if rest else []
        grouped[(kind.lower(), chain, len(parts))].append((name, kind, parts))

    patterns: List[Dict[str, Any]] = []
    for (_kind_key, chain, n_parts), rows in grouped.items():
        kind = rows[0][1]
        if n_parts == 0:
            pattern = f"{kind}{chain}"
        else:
            columns = list(zip(*(row[2] for row in rows)))
            tokens = [
                column[0] if len(set(column)) == 1 else "*" for column in columns
            ]
            pattern = f"{kind}{chain}-" + "-".join(tokens)
        patterns.append(
            {
                "pattern": pattern,
                "count": len(rows),
                "examples": [row[0] for row in rows[:example_limit]],
            }
        )

    if leftovers:
        prefix = os.path.commonprefix(leftovers)
        if len(leftovers) == 1:
            leftover_pattern = leftovers[0]
        elif len(prefix) >= 8:
            leftover_pattern = f"{prefix}*"
        else:
            leftover_pattern = "*"
        patterns.append(
            {
                "pattern": leftover_pattern,
                "count": len(leftovers),
                "examples": leftovers[:example_limit],
            }
        )

    patterns.sort(key=lambda item: (-item["count"], item["pattern"]))
    return patterns


def _split_parser_key(parser_key: str) -> Tuple[str, str]:
    """Split `tiv_taam_price_full_file` into (`TIV_TAAM`, `PRICE_FULL_FILE`)."""
    lower = parser_key.lower()
    file_types = sorted(FileTypesFilters.all_types(), key=len, reverse=True)
    for file_type in file_types:
        suffix = f"_{file_type.lower()}"
        if lower.endswith(suffix):
            return lower[: -len(suffix)].upper(), file_type
    return parser_key.upper(), ""


def compute_improvement_priorities(
    scraper_quality: Dict[str, Any],
    parser_quality: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """Rank scrape/parse gaps that should be fixed next.

    Filter/limit skips are ignored: those are expected, not processing bugs.
    """
    scrape_stats: Dict[str, Dict[str, Any]] = {}
    for iteration in scraper_quality.get("iterations", []):
        for scraper, metrics in (iteration.get("scrapers") or {}).items():
            stats = scrape_stats.setdefault(
                scraper,
                {
                    "iterations": 0,
                    "no_data_iterations": 0,
                    "saw": 0,
                    "downloaded": 0,
                    "download_failed": 0,
                    "skipped_by_filter": 0,
                    "skipped_by_limit": 0,
                    "latest_saw": 0,
                    "latest_downloaded": 0,
                    "latest_no_data": True,
                },
            )
            stats["iterations"] += 1
            stats["saw"] += metrics.get("saw", 0)
            stats["downloaded"] += metrics.get("downloaded", 0)
            if metrics.get("no_data"):
                stats["no_data_iterations"] += 1
            for entry in metrics.get("saw_not_downloaded") or []:
                reason = entry.get("reason") if isinstance(entry, dict) else None
                if reason == "download_failed":
                    stats["download_failed"] += 1
                elif reason == "skipped_by_filter":
                    stats["skipped_by_filter"] += 1
                elif reason == "skipped_by_limit":
                    stats["skipped_by_limit"] += 1
            stats["latest_saw"] = metrics.get("saw", 0)
            stats["latest_downloaded"] = metrics.get("downloaded", 0)
            stats["latest_no_data"] = bool(metrics.get("no_data"))

    scraping: List[Dict[str, Any]] = []
    for scraper, stats in scrape_stats.items():
        if stats["download_failed"] > 0:
            issue = "download_failures"
            why = (
                f"{stats['download_failed']} files failed download or extraction"
            )
        elif (
            stats["iterations"] > 0
            and stats["no_data_iterations"] == stats["iterations"]
        ):
            issue = "no_data"
            why = "no files downloaded"
        elif stats["latest_no_data"] and stats["latest_saw"] > 0:
            issue = "saw_but_not_downloaded"
            why = (
                f"saw {stats['latest_saw']} files but downloaded none"
            )
        else:
            continue
        scraping.append(
            {
                "chain": _dump_stem(scraper),
                "enum": scraper,
                "stage": "scraping",
                "issue": issue,
                "why": why,
                "download_failed": stats["download_failed"],
                "no_data_iterations": stats["no_data_iterations"],
                "iterations": stats["iterations"],
                "latest_saw": stats["latest_saw"],
                "latest_downloaded": stats["latest_downloaded"],
            }
        )
    scraping.sort(
        key=lambda item: (
            item["download_failed"],
            item["no_data_iterations"],
            item["latest_saw"] - item["latest_downloaded"],
        ),
        reverse=True,
    )

    parse_stats: Dict[str, Dict[str, Set[str]]] = {}
    for iteration in parser_quality.get("iterations", []):
        for parser_key, metrics in (iteration.get("parsers") or {}).items():
            stats = parse_stats.setdefault(
                parser_key,
                {"zero_records": set(), "downloaded_not_parsed": set()},
            )
            stats["zero_records"].update(metrics.get("zero_record_files") or [])
            stats["downloaded_not_parsed"].update(
                metrics.get("downloaded_not_parsed") or []
            )

    parsing: List[Dict[str, Any]] = []
    for parser_key, stats in parse_stats.items():
        zero = len(stats["zero_records"])
        unparsed = len(stats["downloaded_not_parsed"])
        if zero == 0 and unparsed == 0:
            continue
        scraper, file_type = _split_parser_key(parser_key)
        if unparsed >= zero:
            issue = "downloaded_not_parsed"
            why = f"{unparsed} downloaded files were never parsed"
        else:
            issue = "zero_records"
            why = f"{zero} parsed files contained zero records"
        examples = sorted(stats["downloaded_not_parsed"] | stats["zero_records"])[:5]
        parsing.append(
            {
                "chain": _dump_stem(scraper),
                "enum": scraper,
                "file_type": file_type,
                "parser_key": parser_key,
                "stage": "parsing",
                "issue": issue,
                "why": why,
                "downloaded_not_parsed": unparsed,
                "zero_records": zero,
                "examples": examples,
            }
        )
    parsing.sort(
        key=lambda item: (
            item["downloaded_not_parsed"] + item["zero_records"],
            item["downloaded_not_parsed"],
        ),
        reverse=True,
    )

    return {"scraping": scraping, "parsing": parsing}


def select_quality_issue_candidates(
    improvements: Dict[str, List[Dict[str, Any]]],
    parse_min_files: int = 50,
) -> Dict[str, List[Dict[str, Any]]]:
    """Turn ranked improvements into GitHub issue candidates.

    Scraping: one candidate per chain (already filtered of limit/filter skips).
    Parsing: one candidate per chain, only if total problem files >= parse_min_files.
    """
    scraping = list(improvements.get("scraping") or [])

    by_chain: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in improvements.get("parsing") or []:
        by_chain[item["chain"]].append(item)

    parsing: List[Dict[str, Any]] = []
    for chain, items in by_chain.items():
        total = sum(
            item.get("downloaded_not_parsed", 0) + item.get("zero_records", 0)
            for item in items
        )
        if total < parse_min_files:
            continue
        items_sorted = sorted(
            items,
            key=lambda item: item.get("downloaded_not_parsed", 0)
            + item.get("zero_records", 0),
            reverse=True,
        )
        parsing.append(
            {
                "chain": chain,
                "enum": items_sorted[0].get("enum"),
                "stage": "parsing",
                "issue": "parse_gaps",
                "total_files": total,
                "by_file_type": items_sorted,
                "fingerprint": f"parsers|{chain}|parse_gaps",
            }
        )
    parsing.sort(key=lambda item: item["total_files"], reverse=True)

    scraping_out = []
    for item in scraping:
        row = dict(item)
        row["stage"] = "scraping"
        row["fingerprint"] = f"scrapers|{item['chain']}|{item['issue']}"
        scraping_out.append(row)

    return {"scraping": scraping_out, "parsing": parsing}


def collect_scrape_problem_filenames(
    scraper_quality: Dict[str, Any],
    scraper_enum: str,
    issue: str,
) -> List[str]:
    """Filenames that belong on a scrape ticket for one chain."""
    names: List[str] = []
    iterations = scraper_quality.get("iterations") or []
    if issue == "download_failures":
        for iteration in iterations:
            metrics = (iteration.get("scrapers") or {}).get(scraper_enum) or {}
            for entry in metrics.get("saw_not_downloaded") or []:
                if not isinstance(entry, dict):
                    continue
                if entry.get("reason") != "download_failed":
                    continue
                file_name = entry.get("file_name")
                if file_name:
                    names.append(normalize_filename(file_name))
    elif issue in {"saw_but_not_downloaded", "no_data"} and iterations:
        metrics = (iterations[-1].get("scrapers") or {}).get(scraper_enum) or {}
        for entry in metrics.get("saw_not_downloaded") or []:
            if not isinstance(entry, dict):
                continue
            if entry.get("reason") == "skipped_by_limit":
                continue
            file_name = entry.get("file_name")
            if file_name:
                names.append(normalize_filename(file_name))
    return list(dict.fromkeys(names))


def collect_scrape_sample_errors(
    scraper_quality: Dict[str, Any],
    scraper_enum: str,
    issue: str,
    limit: int = 5,
) -> List[str]:
    """Distinct download/error messages for the scrape ticket."""
    errors: List[str] = []
    seen: Set[str] = set()
    iterations = scraper_quality.get("iterations") or []
    scan = iterations if issue == "download_failures" else iterations[-1:]
    for iteration in scan:
        metrics = (iteration.get("scrapers") or {}).get(scraper_enum) or {}
        for entry in metrics.get("saw_not_downloaded") or []:
            if not isinstance(entry, dict):
                continue
            if issue == "download_failures" and entry.get("reason") != "download_failed":
                continue
            if entry.get("reason") == "skipped_by_limit":
                continue
            message = entry.get("error")
            if not message or message in seen:
                continue
            seen.add(message)
            errors.append(message)
            if len(errors) >= limit:
                return errors
    return errors


def collect_parse_problem_filenames(
    parser_quality: Dict[str, Any],
    chain: str,
    file_type: Optional[str] = None,
    kind: Optional[str] = None,
) -> List[str]:
    """Unparsed and/or zero-record filenames for one dump-folder chain.

    kind: ``downloaded_not_parsed``, ``zero_records``, or None for both.
    """
    names: List[str] = []
    for iteration in parser_quality.get("iterations") or []:
        for parser_key, metrics in (iteration.get("parsers") or {}).items():
            scraper, parsed_type = _split_parser_key(parser_key)
            if _dump_stem(scraper) != chain:
                continue
            if file_type and parsed_type != file_type:
                continue
            if kind in (None, "downloaded_not_parsed"):
                names.extend(metrics.get("downloaded_not_parsed") or [])
            if kind in (None, "zero_records"):
                names.extend(metrics.get("zero_record_files") or [])
    return list(dict.fromkeys(normalize_filename(name) for name in names if name))


SCRAPER_STARTED_FIELD_ORDER = (
    "status",
    "system_timestamp",
    "task_id",
    "limit",
    "store_id",
    "re_download",
    "files_names_to_scrape",
    "when_date",
    "filter_null",
    "filter_zero",
)
PARSER_STARTED_FIELD_ORDER = (
    "status",
    "system_timestamp",
    "limit",
    "scraper",
    "files_types",
    "task_id",
)


def _started_events_from_metrics(metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = metrics.get("global_status")
    events: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        events = [event for event in raw if isinstance(event, dict)]
    elif isinstance(raw, dict):
        events = [raw]
    elif isinstance(metrics.get("parameters"), dict):
        events = [{"status": "started", **metrics["parameters"]}]
    started = [
        event
        for event in events
        if event.get("status") in (None, "started")
    ]
    return started or events


def _ordered_started_event(
    event: Dict[str, Any],
    field_order: Tuple[str, ...],
) -> Dict[str, Any]:
    """Keep every started-event field, in the status-JSON key order."""
    ordered: Dict[str, Any] = {}
    payload = dict(event)
    if payload.get("status") is None:
        payload["status"] = "started"
    for key in field_order:
        ordered[key] = payload.get(key)
    for key, value in payload.items():
        if key not in ordered:
            ordered[key] = value
    return ordered


def collect_scrape_run_parameters(
    scraper_quality: Dict[str, Any],
    scraper_enum: str,
) -> List[Dict[str, Any]]:
    """Every `global_status` started event for one scraper."""
    started_events: List[Dict[str, Any]] = []
    for iteration in scraper_quality.get("iterations") or []:
        metrics = (iteration.get("scrapers") or {}).get(scraper_enum) or {}
        events = _started_events_from_metrics(metrics)
        if events:
            for event in events:
                started_events.append(
                    {
                        "started": _ordered_started_event(
                            event, SCRAPER_STARTED_FIELD_ORDER
                        )
                    }
                )
            continue
        if metrics.get("task_id"):
            started_events.append(
                {
                    "started": _ordered_started_event(
                        {
                            "status": "started",
                            "system_timestamp": iteration.get("started_at"),
                            "task_id": metrics.get("task_id"),
                        },
                        SCRAPER_STARTED_FIELD_ORDER,
                    )
                }
            )
    return started_events


def collect_parse_run_parameters(
    parser_quality: Dict[str, Any],
    chain: str,
) -> List[Dict[str, Any]]:
    """Every `global_status` started event for one chain's parsers."""
    started_events: List[Dict[str, Any]] = []
    for iteration in parser_quality.get("iterations") or []:
        for parser_key, metrics in (iteration.get("parsers") or {}).items():
            scraper, file_type = _split_parser_key(parser_key)
            if _dump_stem(scraper) != chain:
                continue
            events = _started_events_from_metrics(metrics)
            if events:
                for event in events:
                    started_events.append(
                        {
                            "started": _ordered_started_event(
                                event, PARSER_STARTED_FIELD_ORDER
                            ),
                            "parser_key": parser_key,
                        }
                    )
                continue
            started_events.append(
                {
                    "started": _ordered_started_event(
                        {
                            "status": "started",
                            "system_timestamp": iteration.get("started_at"),
                            "task_id": metrics.get("task_id"),
                            "scraper": scraper,
                            "files_types": file_type,
                        },
                        PARSER_STARTED_FIELD_ORDER,
                    ),
                    "parser_key": parser_key,
                }
            )
    return started_events


def enrich_issue_candidates_with_filename_patterns(
    candidates: Dict[str, List[Dict[str, Any]]],
    scraper_quality: Dict[str, Any],
    parser_quality: Dict[str, Any],
    example_limit: int = 5,
) -> Dict[str, List[Dict[str, Any]]]:
    """Attach generalized filename patterns to scrape/parse issue candidates."""
    for item in candidates.get("scraping") or []:
        names = collect_scrape_problem_filenames(
            scraper_quality, item.get("enum", ""), item.get("issue", "")
        )
        item["filename_patterns"] = generalize_filename_patterns(
            names, example_limit=example_limit
        )
        item["problem_file_count"] = len(names)
        item["sample_errors"] = collect_scrape_sample_errors(
            scraper_quality, item.get("enum", ""), item.get("issue", "")
        )
        item["run_parameters"] = collect_scrape_run_parameters(
            scraper_quality, item.get("enum", "")
        )
    for item in candidates.get("parsing") or []:
        names = collect_parse_problem_filenames(parser_quality, item.get("chain", ""))
        not_parsed = collect_parse_problem_filenames(
            parser_quality, item.get("chain", ""), kind="downloaded_not_parsed"
        )
        zero_rows = collect_parse_problem_filenames(
            parser_quality, item.get("chain", ""), kind="zero_records"
        )
        item["filename_patterns"] = generalize_filename_patterns(
            names, example_limit=example_limit
        )
        item["not_parsed_patterns"] = generalize_filename_patterns(
            not_parsed, example_limit=example_limit
        )
        item["zero_record_patterns"] = generalize_filename_patterns(
            zero_rows, example_limit=example_limit
        )
        item["problem_file_count"] = len(names)
        item["not_parsed_count"] = len(not_parsed)
        item["zero_record_count"] = len(zero_rows)
        for row in item.get("by_file_type") or []:
            typed = collect_parse_problem_filenames(
                parser_quality, item.get("chain", ""), row.get("file_type")
            )
            row["filename_patterns"] = generalize_filename_patterns(
                typed, example_limit=example_limit
            )
        item["run_parameters"] = collect_parse_run_parameters(
            parser_quality, item.get("chain", "")
        )
    return candidates


def _load_health_thresholds() -> Dict[str, float]:
    return load_thresholds()


def compute_unparsed_downloads(
    scraping_status_folder: str,
    converting_status_folder: str,
    enabled_scrapers: List[str],
    enabled_file_types: List[str],
) -> Dict[str, Any]:
    """See :meth:`UnparsedDownloads.compute`."""
    return UnparsedDownloads.compute(
        scraping_status_folder,
        converting_status_folder,
        enabled_scrapers,
        enabled_file_types,
    )


def compute_pipeline_health(
    scraper_quality: Dict[str, Any],
    parser_quality: Dict[str, Any],
    thresholds: Optional[Dict[str, float]] = None,
    unparsed_downloads: Optional[Dict[str, Any]] = None,
    end_to_end_coverage: Optional[Dict[str, Any]] = None,
    download_published_lag: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Derive overall scrape/parse health indicators and threshold checks.

    ``download_published_lag`` is reported only; it has no threshold yet.
    """
    thresholds = thresholds or _load_health_thresholds()
    scraper = scraper_section(scraper_quality)
    parser = parser_section(
        parser_quality, scraper["total_downloaded"]
    )
    if unparsed_downloads is None:
        unparsed_downloads = {"total_files": 0, "by_chain": []}

    sections: Dict[str, Any] = {
        "scraper": scraper,
        "parser": parser,
        UnparsedDownloads.section: dict(unparsed_downloads),
    }
    if end_to_end_coverage is not None:
        sections[EndToEndCoverage.section] = dict(end_to_end_coverage)
    for name, failed in failures_by_section(sections, thresholds).items():
        sections[name]["healthy"] = None if failed is None else not failed
        sections[name]["below_threshold"] = failed or []

    improvements = compute_improvement_priorities(scraper_quality, parser_quality)

    return {
        "computed_at": now().isoformat(),
        "thresholds": thresholds,
        "improvements": improvements,
        "scraper": sections["scraper"],
        "parser": sections["parser"],
        UnparsedDownloads.section: sections[UnparsedDownloads.section],
        EndToEndCoverage.section: sections.get(EndToEndCoverage.section),
        DownloadPublishedLag.section: download_published_lag,
        # None when nothing was judged (no thresholds in the environment).
        "overall_healthy": (
            all(judged) if (judged := [
                section["healthy"]
                for section in sections.values()
                if section["healthy"] is not None
            ]) else None
        ),
    }


def assess_published_health(
    health: Dict[str, Any],
    thresholds: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """Compare a published ``pipeline_health.json`` to the quality thresholds.

    One check per registered indicator. A missing value fails.
    """
    thresholds = thresholds or _load_health_thresholds()
    checks = assess(health, thresholds)
    return {
        "computed_at": health.get("computed_at"),
        "thresholds": thresholds,
        "checks": checks,
        "ok": all(check["ok"] for check in checks),
    }


def _strip_extensions(file_name: str) -> str:
    return file_name.replace(".aspx", "").replace(".xml", "").replace(".gz", "")


def _detected_type_name(file_name: str) -> Optional[str]:
    detected = FileTypesFilters.get_type_from_file(normalize_filename(file_name))
    return detected.name if detected is not None else None


def compute_re_download(
    scraping_status_folder: str,
    converting_status_folder: str,
    enabled_scrapers: List[str],
    enabled_file_types: List[str],
) -> Dict[str, List[str]]:
    """Per chain: files the scraper verified as downloaded that the parser never
    resolved, across every task in the status folders.

    Downloaded comes from the scraper ``verified_downloads`` registry; parsed
    from the parser ``parsed_files`` registry of every enabled file type. Only
    enabled file types are requested, since the parser ignores the rest.

    Names are compared normalized, and returned without extensions: the
    scraper matches each one as a substring of the listed file name.
    """
    requests: Dict[str, List[str]] = {}
    for scraper in enabled_scrapers:
        scraper_raw = _load_status_json(
            os.path.join(
                scraping_status_folder,
                f"{DumpFolderNames[scraper].value.lower()}.json",
            )
        )
        if not scraper_raw:
            continue
        downloaded = {
            row["file_name"]
            for row in scraper_raw.get("verified_downloads") or []
            if row.get("file_name")
            and _detected_type_name(row["file_name"]) in enabled_file_types
        }

        parsed: Set[str] = set()
        for file_type in enabled_file_types:
            parser_key = f"{scraper}_{file_type}".lower()
            parser_raw = _load_status_json(
                os.path.join(converting_status_folder, f"{parser_key}.json")
            )
            for row in (parser_raw or {}).get("parsed_files") or []:
                if row.get("file_name"):
                    parsed.add(normalize_filename(row["file_name"]))

        missing = sorted(
            {
                _strip_extensions(name)
                for name in downloaded
                if normalize_filename(name) not in parsed
            }
        )
        if missing:
            requests[scraper] = missing
    return requests


def write_quality_indicators(
    quality_folder: str,
    scraping_status_folder: str,
    converting_status_folder: str,
    enabled_scrapers: List[str],
    enabled_file_types: List[str],
    outputs_folder: Optional[str] = None,
) -> None:
    """Recompute and persist quality indicator JSON files.

    With ``outputs_folder``, also measures end-to-end coverage against the
    ``file_name`` column of the output CSVs.
    """

    if not os.path.isdir(scraping_status_folder):
        Logger.info(
            "Skipping quality indicators: scraping status folder missing (%s)",
            scraping_status_folder,
        )
        return

    os.makedirs(quality_folder, exist_ok=True)

    scraper_quality = compute_scraper_quality(
        scraping_status_folder, enabled_scrapers
    )
    parser_quality = compute_parser_quality(
        scraping_status_folder,
        converting_status_folder,
        enabled_scrapers,
        enabled_file_types,
    )
    unparsed_downloads = compute_unparsed_downloads(
        scraping_status_folder,
        converting_status_folder,
        enabled_scrapers,
        enabled_file_types,
    )

    scraper_path = os.path.join(quality_folder, SCRAPER_QUALITY_FILENAME)
    parser_path = os.path.join(quality_folder, PARSER_QUALITY_FILENAME)

    end_to_end_coverage = None
    if outputs_folder is not None:
        end_to_end_coverage = EndToEndCoverage.compute(
            scraping_status_folder,
            converting_status_folder,
            outputs_folder,
            enabled_scrapers,
            enabled_file_types,
        )
    download_published_lag = DownloadPublishedLag.compute(
        scraping_status_folder, enabled_scrapers
    )

    pipeline_health = compute_pipeline_health(
        scraper_quality,
        parser_quality,
        unparsed_downloads=unparsed_downloads,
        end_to_end_coverage=end_to_end_coverage,
        download_published_lag=download_published_lag,
    )
    health_path = os.path.join(quality_folder, PIPELINE_HEALTH_FILENAME)

    with open(scraper_path, "w", encoding="utf-8") as handle:
        json.dump(scraper_quality, handle, indent=2, default=str)
    with open(parser_path, "w", encoding="utf-8") as handle:
        json.dump(parser_quality, handle, indent=2, default=str)
    with open(health_path, "w", encoding="utf-8") as handle:
        json.dump(pipeline_health, handle, indent=2, default=str)

    Logger.info(
        "Quality indicators written: scrape_snapshot=%s parse_snapshot=%s "
        "overall_healthy=%s",
        bool(scraper_quality["iterations"]),
        bool(parser_quality["iterations"]),
        pipeline_health["overall_healthy"],
    )
