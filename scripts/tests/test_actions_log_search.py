"""Tests for GitHub Actions log search."""

import zipfile
from datetime import datetime, timezone
from pathlib import Path

from scripts.actions_log_search import (
    classify_line,
    collapse_hits,
    extract_dump_filenames,
    issue_time_window,
    parse_github_log_timestamp,
    parse_issue_ref,
    parse_sas_expiry,
    safe_excerpt_name,
    search_log_zip,
)


ISSUE_SNIPPET = """
Treat files matching these patterns as in-scope.

- `Promo7290027600007-*-*-*-*` (242 files)
  - `Promo7290027600007-002-142-20260902-190000`
  - `Promo7290027600007-002-144-20260902-190000`

- `Price7290027600007-018-750-20260903-014000` (1 files)

--2026-09-02 20:12:39--  https://pricesprodpublic.blob.core.windows.net/promo/Promo7290027600007-002-142-20260902-190000.gz?sv=2014-02-14&sr=b&sig=abc&se=2026-09-02T17%3A34%3A09Z&sp=r
HTTP request sent, awaiting response... 404 The specified blob does not exist.

"system_timestamp": "2026-09-02 07:36:31.737140+03:00"
"""

LOG_LINE = (
    "2026-09-02T17:12:38.1868178Z data-fetcher  | Logger       "
    "2026-09-02 20:12:38,185 DEBUG    Processing "
    "https://pricesprodpublic.blob.core.windows.net/promo/"
    "Promo7290027600007-002-142-20260902-190000.gz?sv=2014-02-14&sr=b"
    "&sig=abc&se=2026-09-02T17:34:09Z&sp=r (in-memory)"
)

FAIL_LINE = (
    "2026-09-02T17:12:39.6879094Z data-fetcher  | Logger       "
    "WARNING  Error downloading "
    "https://pricesprodpublic.blob.core.windows.net/promo/"
    "Promo7290027600007-002-142-20260902-190000.gz?se=2026-09-02T17%3A34%3A09Z: "
    "404 Client Error: The specified blob does not exist."
)

GENERIC_LINE = (
    "2026-09-02T18:01:00.1234567Z data-fetcher  | converting failed for chain yellow"
)


def test_extract_dump_filenames_skips_globs():
    names = extract_dump_filenames(ISSUE_SNIPPET)
    assert "Promo7290027600007-002-142-20260902-190000" in names
    assert "Price7290027600007-018-750-20260903-014000" in names
    assert all("*" not in name for name in names)
    assert "Promo7290027600007" not in names


def test_parse_sas_expiry_unquotes_azure_se():
    expiry = parse_sas_expiry(FAIL_LINE)
    assert expiry == datetime(2026, 9, 2, 17, 34, 9, tzinfo=timezone.utc)


def test_parse_github_log_timestamp_truncates_fraction():
    ts = parse_github_log_timestamp(LOG_LINE)
    assert ts is not None
    assert ts.tzinfo == timezone.utc
    assert ts.hour == 17
    assert ts.minute == 12
    assert ts.second == 38


def test_classify_404_while_sas_still_valid():
    log_ts = parse_github_log_timestamp(FAIL_LINE)
    sas = parse_sas_expiry(FAIL_LINE)
    result = classify_line(FAIL_LINE, log_ts, sas)
    assert result["kind"] == "http_404_blob_missing"
    assert result["sas_expired_at_log_time"] is False
    assert "still valid" in result["hypothesis"]


def test_classify_sas_already_expired():
    line = (
        "2026-09-02T18:00:00.0000000Z 403 AuthenticationFailed "
        "se=2026-09-02T17:34:09Z"
    )
    log_ts = parse_github_log_timestamp(line)
    sas = parse_sas_expiry(line)
    result = classify_line(line, log_ts, sas)
    assert result["kind"] == "http_403_auth"
    assert result["sas_expired_at_log_time"] is True


def test_classify_generic_mention():
    result = classify_line(GENERIC_LINE, parse_github_log_timestamp(GENERIC_LINE), None)
    assert result["kind"] == "mention"
    assert result["sas_expired_at_log_time"] is False


def test_issue_time_window_uses_aware_stamps_not_wget():
    since, until = issue_time_window(ISSUE_SNIPPET)
    assert since is not None and until is not None
    assert since <= datetime(2026, 9, 2, 4, 36, tzinfo=timezone.utc)
    assert until >= datetime(2026, 9, 2, 17, 34, tzinfo=timezone.utc)


def test_parse_issue_ref_url_and_number():
    repo, number = parse_issue_ref(
        "https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers/issues/158",
        "ignored/repo",
    )
    assert repo == "OpenIsraeliSupermarkets/israeli-supermarket-scarpers"
    assert number == "158"
    repo, number = parse_issue_ref("158", "OpenIsraeliSupermarkets/israeli-supermarket-scarpers")
    assert number == "158"
    assert repo.endswith("israeli-supermarket-scarpers")


def test_safe_excerpt_name_for_arbitrary_queries():
    assert safe_excerpt_name("blob does not exist") == "blob_does_not_exist"
    assert safe_excerpt_name("re:ERROR|WARNING") == "re_ERROR_WARNING"


def test_search_log_zip_generic_query(tmp_path: Path):
    zip_path = tmp_path / "run.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "0_call-workflow _ build (3.11.0).txt",
            "\n".join([LOG_LINE, FAIL_LINE, GENERIC_LINE]),
        )
        archive.writestr(
            "call-workflow / build/system.txt",
            "ignore me yellow",
        )

    matches = search_log_zip(
        zip_path,
        ["yellow"],
        run_id=1,
        run_url="https://github.com/example/actions/runs/1",
        max_matches=50,
    )
    assert len(matches) == 1
    assert matches[0].needle == "yellow"
    hits = collapse_hits(matches, tmp_path, sample_limit=2)
    assert hits[0].sas_expires_at is None
    assert hits[0].kinds == ["mention"]


def test_search_log_zip_regex_and_sas_hit(tmp_path: Path):
    zip_path = tmp_path / "run.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "0_call-workflow _ build (3.11.0).txt",
            "\n".join([LOG_LINE, FAIL_LINE, GENERIC_LINE]),
        )

    import re

    matches = search_log_zip(
        zip_path,
        [],
        run_id=33655543740,
        run_url="https://github.com/example/actions/runs/33655543740",
        max_matches=50,
        regexes=[re.compile(r"blob does not exist")],
    )
    assert len(matches) == 1
    hits = collapse_hits(matches, tmp_path, sample_limit=2)
    assert hits[0].needle.startswith("re:")
    assert "http_404_blob_missing" in hits[0].kinds
    assert Path(hits[0].excerpt_path).is_file()
