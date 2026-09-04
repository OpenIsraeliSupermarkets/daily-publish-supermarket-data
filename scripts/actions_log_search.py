#!/usr/bin/env python3
"""Search GitHub Actions workflow logs without dumping them into chat.

Lists runs for a workflow, downloads each run's log zip via `gh`, greps
queries, and prints a compact JSON summary. Full logs stay in --cache-dir.

Use for any workflow and any substring/regex. When a hit contains an Azure
SAS URL, the summary also compares GitHub log UTC to `se=` (stale signed
links vs 404-missing-blob).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import unquote

DEFAULT_ACTIONS_REPO = "OpenIsraeliSupermarkets/daily-publish-supermarket-data"
DEFAULT_ISSUE_REPO = "OpenIsraeliSupermarkets/israeli-supermarket-scarpers"
DEFAULT_CACHE = Path("/tmp/actions-log-search")

DUMP_NAME_RE = re.compile(
    r"\b(?:PriceFull|PromoFull|StoresFull|Price|Promo|Stores)\d+(?:-\d+)+\b"
)
ISSUE_URL_RE = re.compile(
    r"https://github\.com/([^/]+/[^/]+)/issues/(\d+)", re.IGNORECASE
)
GH_LOG_TS_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)Z"
)
SAS_SE_RE = re.compile(r"(?:[?&]|\s)se=([^&\s\"']+)", re.IGNORECASE)
HTTP_STATUS_RE = re.compile(r"\b(40[0134])\b")
AWARE_TS_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})"
)


def parse_github_log_timestamp(line: str) -> Optional[datetime]:
    """UTC timestamp GitHub prefixes onto each Actions log line."""
    match = GH_LOG_TS_RE.match(line.lstrip("\ufeff"))
    if not match:
        return None
    raw = match.group(1)
    if "." in raw:
        head, frac = raw.split(".", 1)
        raw = f"{head}.{frac[:6]:0<6}"
    try:
        return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_sas_expiry(text: str) -> Optional[datetime]:
    """Parse Azure SAS `se=` from a URL or log line, if present."""
    match = SAS_SE_RE.search(text)
    if not match:
        return None
    raw = unquote(match.group(1)).strip().rstrip(".,;:)'\"")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def extract_dump_filenames(text: str) -> List[str]:
    """Dump-folder stems from issue bodies / logs. Skips glob patterns with *."""
    found: List[str] = []
    seen = set()
    for match in DUMP_NAME_RE.finditer(text):
        name = match.group(0)
        if "*" in name or name in seen:
            continue
        seen.add(name)
        found.append(name)
    return found


def parse_aware_timestamps(text: str) -> List[datetime]:
    """Collect timezone-aware timestamps (skip naive wget clocks)."""
    values: List[datetime] = []
    for raw in AWARE_TS_RE.findall(text):
        normalized = raw.replace(" ", "T", 1) if " " in raw[:19] else raw
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        if "." in normalized:
            head, rest = normalized.split(".", 1)
            frac = re.match(r"\d+", rest)
            tz = rest[frac.end() :] if frac else rest
            digits = (frac.group(0) if frac else "0")[:6]
            normalized = f"{head}.{digits}{tz}"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            continue
        values.append(parsed.astimezone(timezone.utc))
    return values


def issue_time_window(
    body: str, pad: timedelta = timedelta(hours=4)
) -> Tuple[Optional[datetime], Optional[datetime]]:
    stamps = parse_aware_timestamps(body)
    sas = parse_sas_expiry(body)
    if sas:
        stamps.append(sas)
    if not stamps:
        return None, None
    return min(stamps) - pad, max(stamps) + pad


def classify_line(
    line: str,
    log_ts: Optional[datetime],
    sas_expires: Optional[datetime],
) -> Dict[str, Any]:
    """Tag a matching line. SAS/HTTP hints are optional extras."""
    lower = line.lower()
    sas_expired = (
        log_ts is not None
        and sas_expires is not None
        and log_ts >= sas_expires
    )
    if "link expired" in lower:
        kind = "sas_html_expired"
        hypothesis = "Azure returned HTML 'link expired' (SAS / listing URL died)."
    elif "authenticationfailed" in lower or (
        "403" in line and "blob" in lower
    ):
        kind = "http_403_auth"
        hypothesis = "Azure rejected the SAS token (expired or invalid signature)."
    elif "blob does not exist" in lower:
        kind = "http_404_blob_missing"
        if sas_expired:
            hypothesis = (
                "404 blob missing, and SAS `se=` had already passed at log time."
            )
        else:
            hypothesis = (
                "Listing advertised a blob Azure no longer has; SAS `se=` was "
                "still valid at download time."
            )
    elif "404" in line:
        kind = "http_404"
        hypothesis = "HTTP 404 on this line."
    elif "file download failed" in lower or "error downloading" in lower:
        kind = "download_error"
        hypothesis = "Download failed; inspect HTTP status and SAS `se=` if present."
    else:
        kind = "mention"
        hypothesis = "Query matched this line."
    return {
        "kind": kind,
        "sas_expired_at_log_time": sas_expired,
        "hypothesis": hypothesis,
    }


def safe_excerpt_name(needle: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", needle).strip("._")
    return (cleaned or "query")[:80]


def _gh(args: Sequence[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["gh", *args],
        check=check,
        capture_output=True,
    )


def parse_issue_ref(
    issue: str, default_repo: str
) -> Tuple[str, str]:
    match = ISSUE_URL_RE.search(issue)
    if match:
        return match.group(1), match.group(2)
    if issue.isdigit():
        return default_repo, issue
    raise ValueError(f"Not an issue number or GitHub issue URL: {issue!r}")


def fetch_issue(repo: str, number: str) -> Dict[str, Any]:
    result = _gh(
        [
            "issue",
            "view",
            number,
            "--repo",
            repo,
            "--json",
            "number,title,body,url",
        ]
    )
    return json.loads(result.stdout.decode("utf-8"))


def list_workflow_runs(
    repo: str,
    workflow: str,
    limit: int,
) -> List[Dict[str, Any]]:
    result = _gh(
        [
            "run",
            "list",
            "--repo",
            repo,
            "--workflow",
            workflow,
            "--limit",
            str(limit),
            "--json",
            "databaseId,startedAt,updatedAt,conclusion,url,event,status",
        ]
    )
    return json.loads(result.stdout.decode("utf-8"))


def run_overlaps(
    run: Dict[str, Any],
    since: Optional[datetime],
    until: Optional[datetime],
) -> bool:
    started = datetime.fromisoformat(run["startedAt"].replace("Z", "+00:00"))
    ended = datetime.fromisoformat(run["updatedAt"].replace("Z", "+00:00"))
    if since and ended < since:
        return False
    if until and started > until:
        return False
    return True


def download_run_logs(repo: str, run_id: int, dest_zip: Path) -> Path:
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    if dest_zip.is_file() and dest_zip.stat().st_size > 0:
        return dest_zip
    result = _gh(
        ["api", f"repos/{repo}/actions/runs/{run_id}/logs"],
        check=False,
    )
    if result.returncode != 0:
        err = result.stderr.decode("utf-8", "replace")
        raise RuntimeError(f"Failed to download logs for run {run_id}: {err}")
    dest_zip.write_bytes(result.stdout)
    return dest_zip


def _line_needle(
    line: str, needles: Sequence[str], regexes: Sequence[re.Pattern[str]]
) -> Optional[str]:
    for name in needles:
        if name in line:
            return name
    for compiled in regexes:
        if compiled.search(line):
            return f"re:{compiled.pattern}"
    return None


@dataclass
class Match:
    run_id: int
    run_url: str
    member: str
    line_no: int
    line: str
    needle: str
    log_ts: Optional[datetime]
    sas_expires: Optional[datetime]
    classification: Dict[str, Any]


@dataclass
class QueryRunHit:
    run_id: int
    run_url: str
    needle: str
    first_log_at: Optional[str]
    sas_expires_at: Optional[str]
    sas_expired_at_first_seen: Optional[bool]
    kinds: List[str]
    http_statuses: List[str]
    hypothesis: str
    match_count: int
    excerpt_path: str
    sample_lines: List[str] = field(default_factory=list)


def search_log_zip(
    zip_path: Path,
    needles: Sequence[str],
    run_id: int,
    run_url: str,
    max_matches: int,
    regexes: Optional[Sequence[re.Pattern[str]]] = None,
) -> List[Match]:
    matches: List[Match] = []
    compiled = list(regexes or [])
    if not needles and not compiled:
        return matches
    with zipfile.ZipFile(zip_path) as archive:
        for info in archive.infolist():
            if info.is_dir() or not info.filename.endswith(".txt"):
                continue
            if Path(info.filename).name == "system.txt":
                continue
            with archive.open(info) as handle:
                for line_no, raw in enumerate(handle, start=1):
                    line = raw.decode("utf-8", "replace").rstrip("\n")
                    needle = _line_needle(line, needles, compiled)
                    if needle is None:
                        continue
                    log_ts = parse_github_log_timestamp(line)
                    sas_expires = parse_sas_expiry(line)
                    matches.append(
                        Match(
                            run_id=run_id,
                            run_url=run_url,
                            member=info.filename,
                            line_no=line_no,
                            line=line,
                            needle=needle,
                            log_ts=log_ts,
                            sas_expires=sas_expires,
                            classification=classify_line(
                                line, log_ts, sas_expires
                            ),
                        )
                    )
                    if len(matches) >= max_matches:
                        return matches
    return matches


def _write_excerpt(path: Path, matches: Iterable[Match]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for match in matches:
        lines.append(f"# {match.member}:{match.line_no}")
        lines.append(match.line)
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def collapse_hits(
    matches: List[Match], cache_dir: Path, sample_limit: int
) -> List[QueryRunHit]:
    grouped: Dict[Tuple[int, str], List[Match]] = {}
    for match in matches:
        grouped.setdefault((match.run_id, match.needle), []).append(match)

    hits: List[QueryRunHit] = []
    for (run_id, needle), group in grouped.items():
        group.sort(key=lambda item: item.line_no)
        first = group[0]
        kinds: List[str] = []
        statuses: List[str] = []
        for item in group:
            kind = item.classification["kind"]
            if kind not in kinds:
                kinds.append(kind)
            for status in HTTP_STATUS_RE.findall(item.line):
                if status not in statuses:
                    statuses.append(status)
        preferred = next(
            (
                item
                for item in group
                if item.classification["kind"] != "mention"
            ),
            first,
        )
        excerpt = (
            cache_dir
            / "excerpts"
            / str(run_id)
            / f"{safe_excerpt_name(needle)}.txt"
        )
        _write_excerpt(excerpt, group[:40])
        first_ts = first.log_ts.isoformat() if first.log_ts else None
        sas = first.sas_expires or preferred.sas_expires
        hits.append(
            QueryRunHit(
                run_id=run_id,
                run_url=first.run_url,
                needle=needle,
                first_log_at=first_ts,
                sas_expires_at=sas.isoformat() if sas else None,
                sas_expired_at_first_seen=(
                    first.log_ts >= sas if first.log_ts and sas else None
                ),
                kinds=kinds,
                http_statuses=statuses,
                hypothesis=preferred.classification["hypothesis"],
                match_count=len(group),
                excerpt_path=str(excerpt),
                sample_lines=[item.line[:400] for item in group[:sample_limit]],
            )
        )
    hits.sort(key=lambda item: (item.first_log_at or "", item.needle))
    return hits


def hit_to_dict(hit: QueryRunHit) -> Dict[str, Any]:
    return {
        "needle": hit.needle,
        "run_id": hit.run_id,
        "run_url": hit.run_url,
        "first_log_at": hit.first_log_at,
        "sas_expires_at": hit.sas_expires_at,
        "sas_expired_at_first_seen": hit.sas_expired_at_first_seen,
        "kinds": hit.kinds,
        "http_statuses": hit.http_statuses,
        "hypothesis": hit.hypothesis,
        "match_count": hit.match_count,
        "excerpt_path": hit.excerpt_path,
        "sample_lines": hit.sample_lines,
    }


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def cmd_list(args: argparse.Namespace) -> int:
    runs = list_workflow_runs(args.repo, args.workflow, args.limit)
    json.dump(
        {"workflow": args.workflow, "repo": args.repo, "runs": runs},
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def cmd_find(args: argparse.Namespace) -> int:
    queries = list(args.query or [])
    issue_meta: Optional[Dict[str, Any]] = None
    since = _parse_iso(args.since)
    until = _parse_iso(args.until)
    regexes = [re.compile(pattern) for pattern in (args.regex or [])]

    if args.issue:
        issue_repo, number = parse_issue_ref(args.issue, args.issue_repo)
        issue_meta = fetch_issue(issue_repo, number)
        body = issue_meta.get("body") or ""
        for name in extract_dump_filenames(body):
            if name not in queries:
                queries.append(name)
        if since is None and until is None:
            since, until = issue_time_window(body)

    if not queries and not regexes:
        raise SystemExit(
            "No search terms. Pass --query, --regex, --file, --pattern, or --issue."
        )

    cache = Path(args.cache_dir)
    cache.mkdir(parents=True, exist_ok=True)

    if args.run_id:
        selected = [
            {
                "databaseId": run_id,
                "startedAt": None,
                "updatedAt": None,
                "url": (
                    f"https://github.com/{args.repo}/actions/runs/{run_id}"
                ),
            }
            for run_id in args.run_id
        ]
    else:
        runs = list_workflow_runs(args.repo, args.workflow, args.limit)
        selected = [run for run in runs if run_overlaps(run, since, until)]

    all_matches: List[Match] = []
    searched: List[Dict[str, Any]] = []
    errors: List[str] = []
    for run in selected:
        run_id = int(run["databaseId"])
        zip_path = cache / "runs" / f"{run_id}.zip"
        try:
            download_run_logs(args.repo, run_id, zip_path)
        except RuntimeError as exc:
            errors.append(str(exc))
            searched.append({**run, "logs": "download_failed"})
            continue
        searched.append(
            {
                "databaseId": run_id,
                "startedAt": run.get("startedAt"),
                "updatedAt": run.get("updatedAt"),
                "url": run.get("url"),
                "logs_zip": str(zip_path),
            }
        )
        all_matches.extend(
            search_log_zip(
                zip_path,
                queries,
                run_id,
                run.get("url") or "",
                max_matches=args.max_matches,
                regexes=regexes,
            )
        )

    hits = collapse_hits(all_matches, cache, sample_limit=args.sample_lines)
    summary: Dict[str, int] = {}
    expired = 0
    live_sas_404 = 0
    for hit in hits:
        for kind in hit.kinds:
            summary[kind] = summary.get(kind, 0) + 1
        if hit.sas_expired_at_first_seen:
            expired += 1
        if (
            "http_404_blob_missing" in hit.kinds
            and hit.sas_expired_at_first_seen is False
        ):
            live_sas_404 += 1

    payload: Dict[str, Any] = {
        "workflow": args.workflow,
        "repo": args.repo,
        "queries": queries,
        "regexes": [rx.pattern for rx in regexes],
        "since": since.isoformat() if since else None,
        "until": until.isoformat() if until else None,
        "issue": issue_meta,
        "runs_searched": searched,
        "errors": errors,
        "summary": {
            "hits": len(hits),
            "sas_expired_at_first_seen": expired,
            "http_404_while_sas_valid": live_sas_404,
            "kinds": summary,
        },
        "hits": [hit_to_dict(hit) for hit in hits],
        "how_to_read": (
            "Do not dump the log zip. Read excerpt_path for extra lines. "
            "GitHub log UTC is first_log_at. "
            "If sas_expires_at is set, compare it to first_log_at: "
            "404 + sas_expired_at_first_seen=false means the listing URL "
            "pointed at a blob that was already gone; "
            "403 / sas_html_expired / sas_expired_at_first_seen=true means "
            "the signed link itself died before download."
        ),
    }
    json.dump(payload, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    return 0 if hits else 2


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--repo",
        default=os.environ.get("ACTIONS_LOG_REPO", DEFAULT_ACTIONS_REPO),
    )
    common.add_argument(
        "--workflow",
        required=True,
        help="Workflow file name, e.g. _prod_scrape.yml or _prod_publishing.yml",
    )
    common.add_argument("--limit", type=int, default=20)
    common.add_argument("--cache-dir", default=str(DEFAULT_CACHE))
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="List runs for a workflow", parents=[common])

    find = sub.add_parser(
        "find",
        help="Search run logs for substrings or regexes",
        parents=[common],
    )
    find.add_argument(
        "--query",
        "-q",
        action="append",
        dest="query",
        default=[],
        help="Substring to find (repeatable)",
    )
    find.add_argument(
        "--file",
        action="append",
        dest="query",
        help="Alias for --query (dump filename or any substring)",
    )
    find.add_argument(
        "--pattern",
        action="append",
        dest="query",
        help="Alias for --query",
    )
    find.add_argument(
        "--regex",
        action="append",
        default=[],
        help="Python regex to find (repeatable)",
    )
    find.add_argument(
        "--run-id",
        action="append",
        type=int,
        help="Search these run ids only (skips workflow list / time filter)",
    )
    find.add_argument(
        "--issue",
        help="GitHub issue number or URL: add dump filenames + time window",
    )
    find.add_argument("--issue-repo", default=DEFAULT_ISSUE_REPO)
    find.add_argument("--since", help="ISO timestamp (UTC ok with Z)")
    find.add_argument("--until", help="ISO timestamp (UTC ok with Z)")
    find.add_argument("--max-matches", type=int, default=500)
    find.add_argument("--sample-lines", type=int, default=3)

    args = parser.parse_args(argv)
    if args.command == "list":
        return cmd_list(args)
    return cmd_find(args)


if __name__ == "__main__":
    raise SystemExit(main())
