---
name: kaggle-quality-issues
description: >-
  Download only published Kaggle quality JSON (scraper_quality.json,
  parser_quality.json, pipeline_health.json) and open deduped GitHub issues on
  israeli-supermarket-scarpers and israeli-supermarket-parsers. Use when filing
  quality gaps, after Kaggle publish, or when the quality-issue-triage agent
  runs. Never download chain zips or CSVs.
---

# Kaggle quality → GitHub issues

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

`plan` prints issue candidates. `apply` creates or comments via `gh`.

## Naming

Quality JSON keys are enums (`TIV_TAAM`). Issue titles use dump-folder stems (`tivtaam`) to match Kaggle folders.

## Fingerprints

Open issues include `<!-- quality-fingerprint: scrapers|shufersal|download_failures -->` (or `parsers|<chain>|parse_gaps`). Same fingerprint → comment, do not open a second issue.
