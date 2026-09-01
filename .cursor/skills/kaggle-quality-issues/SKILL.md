---
name: kaggle-quality-issues
description: >-
  Download only published Kaggle quality JSON (scraper_quality.json,
  parser_quality.json, pipeline_health.json) and open deduped GitHub issues on
  israeli-supermarket-scarpers and israeli-supermarket-parsers. Use when filing
  scrape/parse bugs, after a Kaggle publish, or when the quality-issue-triage
  agent runs. Never download chain zips or CSVs.
---

# Kaggle status JSON → scrape/parse GitHub issues

These tickets are **scraper bugs** and **parser bugs**, not pipeline-health / KPI / quality-score tickets.

## Allowed Kaggle files

Only these remote paths (dataset root):

- `scraper_quality.json`
- `parser_quality.json`
- `pipeline_health.json`

Dataset: `erlichsefi/israeli-supermarkets-2024`

Forbidden: `kagglehub.dataset_download(handle)` with no `path`, any `.csv` / `.zip`, any `chain/file` nested path.

## Script

```bash
python scripts/kaggle_quality_issues.py plan
python scripts/kaggle_quality_issues.py apply --dry-run
python scripts/kaggle_quality_issues.py apply
```

Reuse files already on disk:

```bash
python scripts/kaggle_quality_issues.py plan --cache-dir /tmp/kaggle-il-json
```

`plan` prints issue candidates. `apply` creates issues or **rewrites** matching open issues via `gh`.

## How to write a ticket

Voice: **when I scrape/parse this site with these parameters, I saw these files, the result was X, that is not OK because Y.**

### Scrapers

Collect not-downloaded filenames for that chain from `scraper_quality.json`:

- `download_failures` → every `download_failed` name
- `saw_but_not_downloaded` / `no_data` → latest-iteration names that were not downloaded (`skipped_by_limit` excluded)

Title: `[scrape] {chain}: {pattern} failed to download`

Body:

1. When I scrape `{chain}` with the `global_status` started event(s) below…
2. Full started JSON (all keys: `status`, `system_timestamp`, `task_id`, `limit`, `files_requested`, `store_id`, `files_names_to_scrape`, `when_date`, `filter_null`, `filter_zero`)
3. Files I saw that did not come down (joined pattern + examples)
4. Errors if present
5. Why that is not OK (requested file types should have been downloaded)

### Parsers

Collect `downloaded_not_parsed` and `zero_record_files` separately.

Title: `[parse] {chain}: {pattern} not picked up for parsing` (or `parsed with zero rows`)

Body:

1. When I parse `{chain}` with the started event(s) below…
2. Full started JSON (`status`, `system_timestamp`, `limit`, `scraper`, `files_types`, `task_id`)
3. Files not picked up for parsing
4. Files that parsed with zero rows
5. Why that is not OK (downloaded dumps for that site should emit records)

Do not title or lead with `[quality]`, KPIs, or pipeline-health scores.

## Naming

Quality JSON keys are enums (`TIV_TAAM`). Issue titles use dump-folder stems (`tivtaam`).

## Fingerprints

Open issues include `<!-- quality-fingerprint: scrapers|shufersal|download_failures -->` (or `parsers|<chain>|parse_gaps`). Same fingerprint → update that issue's title and body, do not open a second issue.
