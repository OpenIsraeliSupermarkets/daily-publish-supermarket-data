---
name: actions-log-search
description: >-
  Search GitHub Actions workflow logs via gh without dumping full run output.
  Use when investigating any workflow failure, scrape/parse download errors,
  stale SAS URLs, wget/blob 404s, publishing, or when a quality issue cannot
  be reproduced from Kaggle JSON alone.
---

# Actions log search

Do not paste full Actions logs into chat. Run the script. It writes zips and excerpts under `--cache-dir` and prints a small JSON summary.

Works for **any workflow** in this repo and **any query** (filename, error string, regex). `--issue` is optional: it only adds dump filenames and a time window from a GitHub ticket.

## Script

```bash
python scripts/actions_log_search.py list --workflow _prod_scrape.yml
python scripts/actions_log_search.py find --workflow _prod_scrape.yml --query Promo7290027600007-002-142-20260902-190000
python scripts/actions_log_search.py find --workflow _prod_publishing.yml --query ERROR --since 2026-09-02T00:00:00Z
python scripts/actions_log_search.py find --workflow _prod_scrape.yml --regex "blob does not exist|link expired"
python scripts/actions_log_search.py find --workflow _prod_scrape.yml --run-id 33655543740 --query wget
python scripts/actions_log_search.py find --workflow _prod_scrape.yml --issue 158
python scripts/actions_log_search.py find --workflow _prod_scrape.yml --issue https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers/issues/158
```

Reuse downloads:

```bash
python scripts/actions_log_search.py find --workflow _prod_scrape.yml --query shufersal --cache-dir /tmp/actions-log-search
```

`--file` / `--pattern` are aliases for `--query`. `--workflow` is required. Exit 2 means no hits.

Never `gh run view --log` into the model. Read `excerpt_path` from the JSON if a few extra lines are needed.

## Workflows

| File | When to search |
|---|---|
| `_prod_scrape.yml` | scrape / download / SAS / `data-fetcher` |
| `_prod_publishing.yml` | Kaggle publish |
| `_prod_deploy_api.yml` | production API |
| `template.yml` | reusable job used by prod scrape (search the **caller** workflow) |
| `system_test_pr.yml` | PR system tests |
| `weekly-dependency-bump.yml` | dependency bump |

Repo default: `OpenIsraeliSupermarkets/daily-publish-supermarket-data`. Caller runs include reusable-job logs.

## How to read hits

Use GitHub log UTC (`first_log_at`, the `2026-09-02T17:12:38Z` prefix). Ignore wget's timezone-less `--2026-09-02 20:12:39--`. Production `_prod_scrape.yml` → `template.yml` does **not** set `TZ=Asia/Jerusalem` (system tests do).

When a hit includes a signed Azure URL, extra fields appear:

| Evidence | Meaning |
|---|---|
| 404 blob does not exist **and** `sas_expired_at_first_seen=false` | Listing advertised a blob Azure no longer has. SAS was still live. Local re-scrape often cannot reproduce (new listing, new `se=`). |
| 403 / `AuthenticationFailed` / HTML `link expired` / `sas_expired_at_first_seen=true` | The signed URL died before download. |
| `source corrupt after 3 downloads: extract failed` | Publisher payload is bad; not a link-staleness bug. Close those. |

## After find

Comment with: run URL, matching `needle`, `first_log_at`, and (if present) `sas_expires_at` + HTTP status + `hypothesis`. Do not download Kaggle chain zips or CSVs.
